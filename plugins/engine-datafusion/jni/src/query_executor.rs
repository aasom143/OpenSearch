/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use std::collections::{BTreeSet, HashMap, HashSet};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use jni::sys::jlong;
use datafusion::{
    common::DataFusionError,
    datasource::file_format::parquet::ParquetFormat,
    datasource::listing::{ListingTableUrl, ListingOptions as DataFusionListingOptions, ListingTableConfig as DataFusionListingTableConfig, ListingTable as DataFusionListingTable},
    datasource::object_store::ObjectStoreUrl,
    datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess},
    datasource::physical_plan::ParquetSource,
    execution::cache::cache_manager::CacheManagerConfig,
    execution::cache::cache_unit::DefaultListFilesCache,
    execution::cache::CacheAccessor,
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::TaskContext,
    parquet::arrow::arrow_reader::RowSelector,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    prelude::*,
};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::{Plan, extensions::simple_extension_declaration::MappingType};
use object_store::ObjectMeta;
use prost::Message;
use arrow_schema::{DataType, Field, SchemaRef, SchemaRef as ArrowSchemaRef};
use chrono::TimeZone;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_expr::{LogicalPlan, Projection};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use log::error;
use object_store::path::Path;
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::partial_agg_optimizer::PartialAggregationOptimizer;
use crate::executor::DedicatedExecutor;
use crate::cross_rt_stream::CrossRtStream;
use crate::{CustomFileMeta, FileStats};
use crate::DataFusionRuntime;
use crate::project_row_id_analyzer::ProjectRowIdAnalyzer;
use crate::absolute_row_id_optimizer::{AbsoluteRowIdOptimizer, ROW_BASE_FIELD_NAME, ROW_ID_FIELD_NAME};
use iceberg_datafusion::IcebergTableProviderFactory;
use datafusion::catalog::{TableProviderFactory, TableProvider};
use datafusion::logical_expr::CreateExternalTable;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionState;
use iceberg::{Catalog, CatalogBuilder, TableIdent, NamespaceIdent};
use iceberg_catalog_glue::{GlueCatalogBuilder, GLUE_CATALOG_PROP_WAREHOUSE};
use iceberg_catalog_s3tables::{S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN};

// Import logger macros
use vectorized_exec_spi::{log_info, log_error, log_debug};

/// Enum representing different table source types for query execution
#[derive(Debug, Clone)]
pub enum TableSource {
    /// Local files with explicit metadata
    LocalFiles {
        table_path: ListingTableUrl,
        files_meta: Arc<Vec<CustomFileMeta>>,
    },
    /// Iceberg table from S3 metadata file
    IcebergS3 {
        s3_metadata_path: String,
        s3_options: Option<HashMap<String, String>>,
    },
    /// Iceberg table from AWS Glue Data Catalog
    IcebergGlue {
        database_name: String,
        warehouse_location: String,
        glue_options: Option<HashMap<String, String>>,
    },
    /// Iceberg table from AWS S3 Tables
    IcebergS3Tables {
        table_bucket_arn: String,
        database_name: String,
        s3tables_options: Option<HashMap<String, String>>,
        shard_id: Option<String>,
    },
    /// Downloaded S3 partition files (local ListingTable)
    DownloadedPartition {
        local_dir: String,
    },
}

/// Creates an Iceberg table provider from S3 metadata location.
///
/// # Arguments
/// * `s3_metadata_path` - S3 path to the Iceberg metadata (e.g., "s3://bucket/path/to/metadata/")
/// * `table_name` - Name to register the table under
/// * `state` - DataFusion session state for table creation
/// * `s3_options` - Optional S3 configuration (access key, secret key, region, endpoint, etc.)
///
/// # Returns
/// An Arc-wrapped TableProvider that can be registered in the DataFusion context
///
/// # Example S3 Options
/// ```
/// let mut s3_options = HashMap::new();
/// s3_options.insert("aws_access_key_id".to_string(), "YOUR_ACCESS_KEY".to_string());
/// s3_options.insert("aws_secret_access_key".to_string(), "YOUR_SECRET_KEY".to_string());
/// s3_options.insert("aws_region".to_string(), "us-west-2".to_string());
/// // Optional: for S3-compatible storage like MinIO
/// s3_options.insert("aws_endpoint".to_string(), "http://localhost:9000".to_string());
/// s3_options.insert("aws_allow_http".to_string(), "true".to_string());
/// ```
async fn create_iceberg_table_from_s3(
    s3_metadata_path: &str,
    table_name: &str,
    state: &SessionState,
    s3_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    log_info!("[ICEBERG] Creating Iceberg table from S3: path={}, table={}", s3_metadata_path, table_name);

    // Create the Iceberg table provider factory
    let factory = IcebergTableProviderFactory::new();

    // Prepare options for S3 access
    let options = s3_options.unwrap_or_default();

    // Create the external table command for Iceberg
    // The table name should include namespace for Iceberg compatibility
    let table_ref = datafusion::sql::TableReference::partial("default", table_name);

    let cmd = CreateExternalTable {
        name: table_ref,
        location: s3_metadata_path.to_string(),
        schema: Arc::new(DFSchema::empty()),
        file_type: "iceberg".to_string(),
        options,
        table_partition_cols: vec![],
        order_exprs: vec![],
        constraints: datafusion::common::Constraints::new_unverified(vec![]),
        column_defaults: HashMap::new(),
        if_not_exists: false,
        or_replace: false,
        temporary: false,
        definition: None,
        unbounded: false,
    };

    // Create the table provider using the factory
    let provider = factory.create(state, &cmd).await?;

    log_info!("[ICEBERG] Successfully created Iceberg table provider for {}", table_name);
    Ok(provider)
}

/// Creates an Iceberg table provider from AWS Glue Data Catalog.
///
/// This function uses AWS Glue Data Catalog to discover and load Iceberg tables,
/// which is the recommended approach for production environments as Glue manages
/// the metadata location automatically.
///
/// # Arguments
/// * `database_name` - Glue database name (namespace)
/// * `table_name` - Glue table name
/// * `warehouse_location` - S3 warehouse location (e.g., "s3://bucket/warehouse")
/// * `glue_options` - Optional Glue configuration (region, credentials, catalog_id, etc.)
///
/// # Returns
/// An Arc-wrapped TableProvider that can be registered in the DataFusion context
///
/// # Example Glue Options
/// ```
/// let mut glue_options = HashMap::new();
/// glue_options.insert("aws_region".to_string(), "us-west-2".to_string());
/// glue_options.insert("aws_access_key_id".to_string(), "YOUR_ACCESS_KEY".to_string());
/// glue_options.insert("aws_secret_access_key".to_string(), "YOUR_SECRET_KEY".to_string());
/// // Optional: for cross-account access
/// glue_options.insert("catalog_id".to_string(), "123456789012".to_string());
/// ```
async fn create_iceberg_table_from_glue(
    database_name: &str,
    table_name: &str,
    warehouse_location: &str,
    glue_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    log_info!("[ICEBERG-GLUE] Creating Iceberg table from Glue: database={}, table={}", database_name, table_name);

    // Prepare Glue catalog properties
    let mut catalog_props = HashMap::new();
    catalog_props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_location.to_string());

    // Add user-provided options
    if let Some(opts) = glue_options {
        catalog_props.extend(opts);
    }

    // Create Glue catalog
    let glue_catalog = GlueCatalogBuilder::default()
        .load("glue_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-GLUE] Glue catalog created successfully");

    // Load table from Glue
    let namespace = NamespaceIdent::new(database_name.to_string());
    let table_ident = TableIdent::new(namespace, table_name.to_string());

    let iceberg_table = glue_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-GLUE] Table loaded from Glue: {}.{}", database_name, table_name);

    // Get the metadata location from the loaded table
    // Note: For Glue tables, we need to get the actual metadata file location
    // from the table's metadata, not just the table location
    let metadata = iceberg_table.metadata();
    let metadata_location = iceberg_table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();

    log_info!("[ICEBERG-GLUE] Table location: {}", metadata.location());
    log_info!("[ICEBERG-GLUE] Metadata location: {}", metadata_location);

    // Now create the DataFusion table provider using the metadata location
    // This is similar to the S3 approach but the metadata location comes from Glue
    let factory = IcebergTableProviderFactory::new();

    // Create a minimal session state for the factory
    let ctx = SessionContext::new();
    let state = ctx.state();

    let table_ref = datafusion::sql::TableReference::partial("default", table_name);
    let cmd = CreateExternalTable {
        name: table_ref,
        location: metadata_location,
        schema: Arc::new(DFSchema::empty()),
        file_type: "iceberg".to_string(),
        options: HashMap::new(),
        table_partition_cols: vec![],
        order_exprs: vec![],
        constraints: datafusion::common::Constraints::new_unverified(vec![]),
        column_defaults: HashMap::new(),
        if_not_exists: false,
        or_replace: false,
        temporary: false,
        definition: None,
        unbounded: false,
    };

    let provider = factory.create(&state, &cmd).await?;

    log_info!("[ICEBERG-GLUE] Successfully created Iceberg table provider for {}.{}", database_name, table_name);
    Ok(provider)
}

/// Creates an Iceberg table provider from AWS S3 Tables.
///
/// AWS S3 Tables is a managed service for Apache Iceberg tables that provides
/// optimized storage and query performance with automatic metadata management.
///
/// # Arguments
/// * `table_bucket_arn` - ARN of the S3 Tables bucket (e.g., "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket")
/// * `database_name` - Namespace/database name in S3 Tables
/// * `table_name` - Table name in S3 Tables
/// * `s3tables_options` - Optional S3 Tables configuration (region, credentials, endpoint, etc.)
///
/// # Returns
/// An Arc-wrapped TableProvider that can be registered in the DataFusion context
///
/// # Example S3 Tables Options
/// ```
/// let mut s3tables_options = HashMap::new();
/// s3tables_options.insert("aws_region".to_string(), "us-west-2".to_string());
/// s3tables_options.insert("aws_access_key_id".to_string(), "YOUR_ACCESS_KEY".to_string());
/// s3tables_options.insert("aws_secret_access_key".to_string(), "YOUR_SECRET_KEY".to_string());
/// // Optional: for local testing
/// s3tables_options.insert("endpoint_url".to_string(), "http://localhost:4566".to_string());
/// ```
async fn create_iceberg_table_from_s3tables(
    table_bucket_arn: &str,
    database_name: &str,
    table_name: &str,
    s3tables_options: Option<HashMap<String, String>>,
    shard_id: Option<String>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    log_info!("[ICEBERG-S3TABLES] Creating Iceberg table from S3 Tables: bucket={}, database={}, table={}",
              table_bucket_arn, database_name, table_name);

    // Prepare S3 Tables catalog properties
    let mut catalog_props = HashMap::new();
    catalog_props.insert(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(), table_bucket_arn.to_string());

    // Add user-provided options
    if let Some(opts) = s3tables_options {
        catalog_props.extend(opts);
    }

    // Create S3 Tables catalog
    let s3tables_catalog = S3TablesCatalogBuilder::default()
        .load("s3tables_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-S3TABLES] S3 Tables catalog created successfully");

    // Load table from S3 Tables
    let namespace = NamespaceIdent::new(database_name.to_string());
    let table_ident = TableIdent::new(namespace, table_name.to_string());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-S3TABLES] Table loaded from S3 Tables: {}.{}", database_name, table_name);

    // Get the metadata location from the loaded table
    let metadata_location = iceberg_table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();

    log_info!("[ICEBERG-S3TABLES] Metadata location: {}", metadata_location);

    // Create the DataFusion table provider using the metadata location
    let factory = IcebergTableProviderFactory::new();

    // Create a minimal session state for the factory
    let ctx = SessionContext::new();
    let state = ctx.state();

    let table_ref = datafusion::sql::TableReference::partial("default", table_name);

    let cmd = CreateExternalTable {
        name: table_ref,
        location: metadata_location,
        schema: Arc::new(DFSchema::empty()),
        file_type: "iceberg".to_string(),
        options: HashMap::new(),
        table_partition_cols: vec![],
        order_exprs: vec![],
        constraints: datafusion::common::Constraints::new_unverified(vec![]),
        column_defaults: HashMap::new(),
        if_not_exists: false,
        or_replace: false,
        temporary: false,
        definition: None,
        unbounded: false,
    };
    log_info!("[DEBUG] CreateExternalTable cmd created");
    // table_partition_cols: vec!["shard_id".to_string()],

    let provider = factory.create(&state, &cmd).await?;

    log_info!("[ICEBERG-S3TABLES] Successfully created Iceberg table provider for {}.{}", database_name, table_name);
    log_info!("[DEBUG] Iceberg provider schema fields: {:?}",
              provider.schema().fields().iter().map(|f| (f.name(), f.data_type())).collect::<Vec<_>>());

    // Check if shard_id filtering is requested
    if let Some(shard) = &shard_id {
        if provider.schema().field_with_name("shard_id").is_ok() {
            log_info!("[DEBUG] shard_id column found in schema, partition filtering will be applied via logical plan");
        } else {
            log_info!("[DEBUG] WARNING: shard_id column NOT found in schema");
            log_info!("[DEBUG] This means the Parquet files don't contain shard_id as a data column");
            log_info!("[DEBUG] Iceberg partition pruning requires shard_id to be in the data schema OR");
            log_info!("[DEBUG] you need to use Iceberg's scan API with partition filters");
            log_info!("[DEBUG] Current workaround: Ensure Parquet files include shard_id column during write");
        }
    }

    Ok(provider)
}

/// Helper function to inject a filter at the TableScan level of a logical plan.
/// This ensures the filter is applied before any aggregations or projections.
fn inject_filter_at_table_scan(plan: LogicalPlan, filter_expr: Expr) -> Result<LogicalPlan, DataFusionError> {
    // Transform the plan recursively to find TableScan and inject filter
    plan.transform(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                // Found a TableScan - wrap it with a Filter
                let filter_plan = LogicalPlan::Filter(datafusion_expr::Filter::try_new(
                    filter_expr.clone(),
                    Arc::new(LogicalPlan::TableScan(scan)),
                )?);
                Ok(Transformed::yes(filter_plan))
            }
            _ => {
                // Not a TableScan - continue traversing
                Ok(Transformed::no(node))
            }
        }
    })
    .map(|transformed| transformed.data)
}


/// Executes a query using DataFusion with cross-runtime streaming capabilities.
/// This function sets up the complete query execution pipeline including table registration,
/// plan optimization, and stream creation for efficient data processing across different runtimes.
///
/// # Arguments
/// * `table_source` - The source of the table data (local files, Iceberg S3, Glue, or S3 Tables)
/// * `table_name` - Name to register the table under in the DataFusion context
/// * `plan_bytes_vec` - Serialized Substrait query plan as bytes
/// * `is_query_plan_explain_enabled` - Flag to enable query plan explanation
/// * `runtime` - The DataFusion runtime environment containing configuration and caches
/// * `cpu_executor` - Dedicated executor for CPU-intensive operations
///
/// # Returns
/// A pointer (as jlong) to the cross-runtime stream that can be consumed from Java/JNI
///
/// # Table Source Types
/// - `LocalFiles`: Traditional approach with explicit file metadata
/// - `IcebergS3`: Direct S3 metadata file path
/// - `IcebergGlue`: AWS Glue Data Catalog integration
/// - `IcebergS3Tables`: AWS S3 Tables integration
pub async fn execute_query_with_cross_rt_stream(
    table_source: TableSource,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    log_info!("[FLOW] execute_query_with_cross_rt_stream: tableName={}, source={:?}", table_name, table_source);

    // Set AWS credentials if using Iceberg (temporary hardcoded for testing)
    match &table_source {
        TableSource::IcebergS3 { .. }
        | TableSource::IcebergGlue { .. }
        | TableSource::IcebergS3Tables { .. } => {
            std::env::set_var("AWS_REGION", "us-west-2");
            std::env::set_var("AWS_ACCESS_KEY_ID", "<access-key>");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "<secret-key>");
            std::env::set_var("AWS_SESSION_TOKEN", "<token>");
            log_info!("[FLOW] AWS credentials set programmatically (hardcoded)");
        }
        _ => {}
    }

    let runtimeEnv = &runtime.runtime_env;

    // Setup runtime environment based on table source type
    let runtime_env = match &table_source {
        TableSource::LocalFiles { table_path, files_meta } => {
            // For local files, set up file caching
            let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
                files_meta
                    .iter()
                    .map(|metadata| (*metadata.object_meta).clone())
                    .collect(),
            );

            let list_file_cache = Arc::new(DefaultListFilesCache::default());
            list_file_cache.put(table_path.prefix(), object_meta);

            RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
                .with_cache_manager(
                    CacheManagerConfig::default()
                        .with_list_files_cache(Some(list_file_cache.clone()))
                        .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache()))
                        .with_files_statistics_cache(runtimeEnv.cache_manager.get_file_statistic_cache()),
                )
                .with_metadata_cache_limit(250 * 1024 * 1024)
                .build()?
        }
        _ => {
            // For Iceberg sources, simpler runtime setup
            RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
                .with_cache_manager(
                    CacheManagerConfig::default()
                        .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache()))
                        .with_files_statistics_cache(runtimeEnv.cache_manager.get_file_statistic_cache()),
                )
                .with_metadata_cache_limit(250 * 1024 * 1024)
                .build()?
        }
    };

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.target_partitions = 1;
    config.options_mut().execution.batch_size = 8192;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config.clone())
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(PartialAggregationOptimizer))
        .build();

    let ctx = SessionContext::new_with_state(state.clone());

    // Register table based on source type
    let provider: Arc<dyn TableProvider> = match &table_source {
        TableSource::LocalFiles { table_path, files_meta } => {
            log_info!("[FLOW] Registering table from local files: fileCount={}", files_meta.len());

            let file_format = ParquetFormat::new();
            let listing_options = ListingOptions::new(Arc::new(file_format))
                .with_file_extension(".parquet")
                .with_files_metadata(files_meta.clone())
                .with_session_config_options(&config)
                .with_collect_stat(true)
                .with_table_partition_cols(vec![(ROW_BASE_FIELD_NAME.to_string(), DataType::Int64)]);

            let resolved_schema = listing_options.infer_schema(&ctx.state(), table_path).await?;
            let table_config = ListingTableConfig::new(table_path.clone())
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);

            Arc::new(ListingTable::try_new(table_config)?)
        }
        TableSource::IcebergS3 { s3_metadata_path, s3_options } => {
            log_info!("[FLOW] Registering table from Iceberg S3: path={}", s3_metadata_path);
            create_iceberg_table_from_s3(s3_metadata_path, &table_name, &state, s3_options.clone()).await?
        }
        TableSource::IcebergGlue { database_name, warehouse_location, glue_options } => {
            log_info!("[FLOW] Registering table from Iceberg Glue: database={}, table={}", database_name, table_name);
            create_iceberg_table_from_glue(database_name, &table_name, warehouse_location, glue_options.clone()).await?
        }
        TableSource::IcebergS3Tables { table_bucket_arn, database_name, s3tables_options, shard_id } => {
            log_info!("[FLOW] Registering table from Iceberg S3 Tables: bucket={}, database={}, table={}, shard_id={:?}",
                     table_bucket_arn, database_name, table_name, shard_id);
            create_iceberg_table_from_s3tables(table_bucket_arn, database_name, &table_name, s3tables_options.clone(), shard_id.clone()).await?
        }
        TableSource::DownloadedPartition { local_dir } => {
            log_info!("[FLOW] Registering table from downloaded partition: local_dir={}", local_dir);
            
            // Create ListingTable from local directory
            let local_url = format!("file://{}", local_dir);
            let table_path = ListingTableUrl::parse(&local_url)?;

            let file_format = Arc::new(ParquetFormat::default());
            let listing_options = DataFusionListingOptions::new(file_format)
                .with_file_extension(".parquet")
                .with_collect_stat(true);

            let inferred_schema = listing_options.infer_schema(&state, &table_path).await?;
            log_info!("[FLOW] Inferred schema with {} fields from downloaded files", inferred_schema.fields().len());

            let listing_config = DataFusionListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(inferred_schema);

            Arc::new(DataFusionListingTable::try_new(listing_config)?)
        }
    };

    ctx.register_table(&table_name, provider.clone())?;
    log_info!("[FLOW] Table '{}' registered successfully", table_name);
    log_info!("[SCHEMA-DEBUG] Registered table '{}' schema:", table_name);
    for (idx, field) in provider.schema().fields().iter().enumerate() {
        log_info!("[SCHEMA-DEBUG]   Field[{}]: name='{}', type={:?}", idx, field.name(), field.data_type());
    }

    log_info!("[FLOW] Decoding Substrait plan");
    // Decode substrait
    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to decode Substrait plan: {}", e);
            return Err(DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)));
        }
    };
    log_info!("[DEBUG] Substrait plan decoded successfully");

    // Log Substrait plan details for debugging schema mismatch
    if let Some(relations) = &substrait_plan.relations.first() {
        if let Some(rel_type) = &relations.rel_type {
            log_info!("[SCHEMA-DEBUG] Substrait relation type: {:?}", rel_type);
        }
    }

    // Log Substrait extensions (function mappings)
    log_info!("[SCHEMA-DEBUG] Substrait extensions count: {}", substrait_plan.extensions.len());
    for (idx, ext) in substrait_plan.extensions.iter().enumerate() {
        if let Some(mapping_type) = &ext.mapping_type {
            log_info!("[SCHEMA-DEBUG]   Extension[{}]: {:?}", idx, mapping_type);
        }
    }

    let mut modified_plan = substrait_plan.clone();
    
    // Fix function name mappings
    for ext in modified_plan.extensions.iter_mut() {
        if let Some(mapping_type) = &mut ext.mapping_type {
            if let MappingType::ExtensionFunction(func) = mapping_type {
                if func.name == "approx_count_distinct:any" {
                    func.name = "approx_distinct:any".to_string();
                }
            }
        }
    }

    let mut logical_plan = match from_substrait_plan(&ctx.state(), &modified_plan).await {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to convert Substrait plan: {}", e);
            log_info!("[SCHEMA-DEBUG] Available fields in table provider:");
            for (idx, field) in provider.schema().fields().iter().enumerate() {
                log_info!("[SCHEMA-DEBUG]   Field[{}]: name='{}', type={:?}", idx, field.name(), field.data_type());
            }
            log_info!("[SCHEMA-DEBUG] Substrait plan conversion failed. This usually means:");
            log_info!("[SCHEMA-DEBUG] 1. The SQL query references fields that don't exist in the Parquet files");
            log_info!("[SCHEMA-DEBUG] 2. The Substrait plan contains computed fields (like 'score') that aren't in the data");
            log_info!("[SCHEMA-DEBUG] 3. Field name mismatch between query and actual schema");
            return Err(e);
        }
    };

    // Inject partition filter into logical plan
    // This must be done BEFORE aggregations, so we inject it at the TableScan level
    match &table_source {
        TableSource::IcebergS3Tables { shard_id: Some(shard), .. } => {
            if provider.schema().field_with_name("shard_id").is_ok() {
                let shard_value: i32 = shard.parse().unwrap_or(0);
                let filter_expr = col("shard_id").eq(lit(shard_value));
                log_info!("[PARTITION-INJECT] Injecting filter into logical plan: shard_id = {}", shard_value);
                
                // Inject filter at the TableScan level by transforming the plan
                logical_plan = inject_filter_at_table_scan(logical_plan, filter_expr)?;
            } else {
                log_info!("[PARTITION-INJECT] WARNING: shard_id field not found in schema, skipping filter injection");
            }
        }
        _ => {}
    }

    // Log logical plan schema for debugging
    log_info!("[SCHEMA-DEBUG] Logical plan schema after Substrait conversion:");
    let schema = logical_plan.schema();
    for (idx, field) in schema.fields().iter().enumerate() {
        log_info!("[SCHEMA-DEBUG]   Field[{}]: name='{}', type={:?}", idx, field.name(), field.data_type());
    }

    log_info!("[DEBUG] Logical plan: {}", logical_plan.display_indent());

    let is_aggregation_query = is_aggs_query(&logical_plan);

    log_info!("[FLOW] Query type: isAggregation={}", is_aggregation_query);
    // For non-aggregation queries, we apply a two-phase optimization strategy to ensure
    // only absolute row IDs are returned, which is essential for subsequent fetch operations
    if !is_aggregation_query {
        // Phase 1: ProjectRowIdAnalyzer (Logical Plan Analysis)
        // This analyzer works at the logical plan level and ensures that:
        // 1. TableScan nodes include the ___row_id field in their projections
        // 2. Projection nodes propagate the ___row_id field through the query tree
        // 3. The ___row_id field is available at every level of the plan for later optimization
        log_info!("[FLOW] Applying ProjectRowIdAnalyzer for non-aggregation query");
        logical_plan = ProjectRowIdAnalyzer.analyze(logical_plan, ctx.state().config_options())?;

        // Phase 2: Top-level Projection Restriction
        // Create a final projection that ONLY selects the ___row_id field
        // This ensures the query result contains only the row identifiers needed for the fetch phase
        // The AbsoluteRowIdOptimizer (applied earlier) will later transform these relative IDs
        // into absolute IDs during physical plan execution.
        // Creation of final projection is needed since in some case top-level projection is missing
        // from the plan if final projection schema matches downstream exec schemas, making
        // projection exec redundant.
        // OptimizeProjections LogicalPlan optimizer is applied during execution which removes any
        // additional projection execs are present.
        logical_plan = LogicalPlan::Projection(Projection::try_new(
            vec![col(ROW_ID_FIELD_NAME.to_string())],
            Arc::new(logical_plan),
        ).expect("Failed to create top level projection with ___row_id"));
    }

    let mut dataframe = match ctx.execute_logical_plan(logical_plan).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to execute logical plan: {}", e);
            return Err(e);
        }
    };

    let mut physical_plan = dataframe.clone().create_physical_plan().await?;

    // Log physical plan schema for debugging
    log_info!("[SCHEMA-DEBUG] Physical plan schema:");
    for (idx, field) in physical_plan.schema().fields().iter().enumerate() {
        log_info!("[SCHEMA-DEBUG]   Field[{}]: name='{}', type={:?}", idx, field.name(), field.data_type());
    }

    log_info!("[FLOW] Physical plan created");
    // For non-aggregation queries, we need to return absolute row IDs to identify specific rows
    // The AbsoluteRowIdOptimizer works at the physical plan level to transform relative row IDs
    // into absolute ones by adding the partition's row_base offset
    if !is_aggregation_query {
        // AbsoluteRowIdOptimizer: Transforms ___row_id expressions in the physical plan
        // It finds expressions that reference ___row_id and replaces them with:
        // ___row_id + row_base (where row_base comes from partition columns)
        // This converts file-relative row IDs to globally unique absolute row IDs
        log_info!("[FLOW] Applying AbsoluteRowIdOptimizer");
        physical_plan = AbsoluteRowIdOptimizer.optimize(physical_plan, ctx.state().config_options())
            .expect("Failed to optimize physical plan");
    }


    if is_query_plan_explain_enabled {
        println!("---- Explain plan ----");
        let clone_df = dataframe.clone().explain(false, true).expect("Failed to explain plan");
        clone_df.show().await?;
    }

    let df_stream = match execute_stream(physical_plan, ctx.task_ctx()) {
        Ok(stream) => stream,
        Err(e) => {
            error!("Failed to create execution stream: {}", e);
            return Err(e);
        }
    };

    log_info!("[FLOW] Execution stream created, returning cross-runtime stream");
    Ok(get_cross_rt_stream(cpu_executor, df_stream))
}

/// Backward compatibility wrapper for local file execution
pub async fn execute_query_with_cross_rt_stream_local_files(
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<CustomFileMeta>>,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    execute_query_with_cross_rt_stream(
        TableSource::LocalFiles { table_path, files_meta },
        table_name,
        plan_bytes_vec,
        is_query_plan_explain_enabled,
        runtime,
        cpu_executor,
    ).await
}

/// Convenience wrapper for Iceberg S3 metadata file execution
pub async fn execute_query_with_iceberg_from_s3(
    s3_metadata_path: String,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    s3_options: Option<HashMap<String, String>>,
) -> Result<jlong, DataFusionError> {
    execute_query_with_cross_rt_stream(
        TableSource::IcebergS3 { s3_metadata_path, s3_options },
        table_name,
        plan_bytes_vec,
        is_query_plan_explain_enabled,
        runtime,
        cpu_executor,
    ).await
}

/// Convenience wrapper for Iceberg Glue catalog execution
pub async fn execute_query_with_iceberg_from_glue(
    database_name: String,
    table_name: String,
    warehouse_location: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    glue_options: Option<HashMap<String, String>>,
) -> Result<jlong, DataFusionError> {
    execute_query_with_cross_rt_stream(
        TableSource::IcebergGlue { database_name, warehouse_location, glue_options },
        table_name,
        plan_bytes_vec,
        is_query_plan_explain_enabled,
        runtime,
        cpu_executor,
    ).await
}

/// Convenience wrapper for Iceberg S3 Tables execution
pub async fn execute_query_with_iceberg_from_s3tables(
    table_bucket_arn: String,
    database_name: String,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    s3tables_options: Option<HashMap<String, String>>,
    shard_id: Option<String>,
) -> Result<jlong, DataFusionError> {
    execute_query_with_cross_rt_stream(
        TableSource::IcebergS3Tables { table_bucket_arn, database_name, s3tables_options, shard_id },
        table_name,
        plan_bytes_vec,
        is_query_plan_explain_enabled,
        runtime,
        cpu_executor,
    ).await
}


pub fn get_cross_rt_stream(cpu_executor: DedicatedExecutor, df_stream: SendableRecordBatchStream) -> jlong {
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(
        df_stream,
        cpu_executor,
    );

    let wrapped_stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Box::into_raw(Box::new(wrapped_stream)) as jlong
}

/// Executes a query using Iceberg tables from S3 with cross-runtime streaming capabilities.
/// This function provides an alternative to `execute_query_with_cross_rt_stream` that uses
/// Iceberg metadata from S3 instead of local file listings.
///
/// # Arguments
/// * `s3_metadata_path` - S3 path to the Iceberg table metadata file
///   Example: "s3://bucket/warehouse/db/table/metadata/00005-uuid.metadata.json"
/// * `table_name` - Name to register the table under in the DataFusion context
/// * `plan_bytes_vec` - Serialized Substrait query plan as bytes
/// * `is_query_plan_explain_enabled` - Flag to enable query plan explanation
/// * `runtime` - The DataFusion runtime environment containing configuration and caches
/// * `cpu_executor` - Dedicated executor for CPU-intensive operations
/// * `s3_options` - Optional S3 configuration (credentials, region, endpoint, etc.)
///
/// # Returns
/// A pointer (as jlong) to the cross-runtime stream that can be consumed from Java/JNI
///
/// # S3 Configuration
/// The s3_options HashMap can include (using aws.* prefix):
/// - aws.access_key_id: AWS access key
/// - aws.secret_access_key: AWS secret key
/// - aws.session_token: AWS session token (for temporary credentials)
/// - aws.region: AWS region (e.g., "us-west-2")
/// - aws.endpoint: Custom S3 endpoint for S3-compatible storage
/// - aws.allow_http: "true" to allow HTTP connections
///
/// # Note
/// Currently requires AWS credentials to be set in environment variables.
/// The s3_options parameter is prepared for future use when iceberg-datafusion
/// properly passes options to the FileIO builder.
///
/// # Example
/// ```rust
/// let mut s3_options = HashMap::new();
/// s3_options.insert("aws.region".to_string(), "us-west-2".to_string());
///
/// let stream_ptr = execute_query_with_iceberg_from_s3(
///     "s3://bucket/warehouse/db/table/metadata/00005-uuid.metadata.json".to_string(),
///     "my_table".to_string(),
///     substrait_plan_bytes,
/// Executes the fetch phase of a two-phase query execution strategy.
/// This function takes absolute row IDs (returned from the query phase) and efficiently
/// retrieves the actual row data using Parquet's row-level access capabilities.
///
/// # Two-Phase Query Execution Strategy
///
/// **Phase 1 (Query):** `execute_query_with_cross_rt_stream`
/// - Applies filters and conditions to identify matching rows
/// - Returns only absolute row IDs (___row_id) for matching rows
/// - Uses optimizers to ensure row IDs are absolute (not file-relative)
///
/// **Phase 2 (Fetch):** This function
/// - Takes the absolute row IDs from phase 1
/// - Creates optimized Parquet access plans for targeted row retrieval
/// - Fetches only the requested columns for the identified rows
/// - Reconstructs absolute row IDs by adding row_base back to relative IDs
///
/// # Arguments
/// * `table_path` - The URL path to the table data source
/// * `files_metadata` - Metadata for all files including row counts and base offsets
/// * `row_ids` - Absolute row IDs to fetch (from query phase)
/// * `include_fields` - Specific fields to include in the result
/// * `exclude_fields` - Fields to exclude from the result
/// * `runtime` - The DataFusion runtime environment
/// * `cpu_executor` - Dedicated executor for CPU-intensive operations
///
/// # Returns
/// A pointer (as jlong) to the cross-runtime stream containing the fetched row data
///
/// # Optimization Details
/// - Uses ParquetAccessPlan for efficient row-group level access
/// - Skips entire row groups that don't contain target rows
/// - Uses RowSelector for precise row-level filtering within row groups
/// - Reconstructs absolute row IDs using row_base + relative_row_id calculation
pub async fn execute_fetch_phase(
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
    row_ids: Vec<jlong>,
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    log_info!("[FLOW] execute_fetch_phase: rowIdCount={}, includeFields={:?}", row_ids.len(), include_fields);
    // Create optimized Parquet access plans for targeted row retrieval
    // This converts absolute row IDs back to file-relative positions and creates
    // efficient access patterns for each file's row groups
    let access_plans = create_access_plans(row_ids, files_metadata.clone()).await?;

    let object_meta: Arc<Vec<ObjectMeta>> = Arc::new(
        files_metadata
            .iter()
            .map(|metadata| (*metadata.object_meta).clone())
            .collect(),
    );

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), object_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(
            CacheManagerConfig::default().with_list_files_cache(Some(list_file_cache))
                         .with_metadata_cache_limit(runtime.runtime_env.cache_manager.get_file_metadata_cache().cache_limit())
                .with_file_metadata_cache(Some(runtime.runtime_env.cache_manager.get_file_metadata_cache().clone()))
                .with_files_statistics_cache(runtime.runtime_env.cache_manager.get_file_statistic_cache()),

        )
        .build()?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet").with_collect_stat(true);

    let parquet_schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
    let projections = create_projections(include_fields, exclude_fields, parquet_schema.clone());

    let partitioned_files: Vec<PartitionedFile> = files_metadata
        .iter()
        .zip(access_plans.iter())
        .map(|(meta, access_plan)| {
            PartitionedFile {
                object_meta:  ObjectMeta {
                    location: Path::from(meta.object_meta().location.to_string()),
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    size: meta.object_meta.size,
                    e_tag: None,
                    version: None,
                },
                partition_values: vec![ScalarValue::Int64(Some(*meta.row_base))],
                range: None,
                statistics: None,
                extensions: None,
                metadata_size_hint: None,
            }
                .with_extensions(Arc::new(access_plan.clone()))
        })
        .collect();

    let file_group = FileGroup::new(partitioned_files);
    let file_source = Arc::new(ParquetSource::default());

    let mut projection_index = vec![];
    for field_name in projections.iter() {
        projection_index.push(
            parquet_schema
                .index_of(field_name)
                .map_err(|_| DataFusionError::Execution(format!("Projected field {} not found in Schema", field_name)))?,
        );
    }

    // Ensure ___row_id is always included in projections for absolute row ID reconstruction
    // Even if not explicitly requested, we need it to rebuild absolute row IDs
    if(!projections.contains(&ROW_ID_FIELD_NAME.to_string())) {
        projection_index.push(parquet_schema.index_of(ROW_ID_FIELD_NAME).unwrap());
    }
    // Add row_base partition column index for absolute row ID calculation
    projection_index.push(parquet_schema.fields.len());

    let file_scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        parquet_schema.clone(),
        file_source,
    )
    .with_table_partition_cols(vec![Field::new(ROW_BASE_FIELD_NAME, DataType::Int64, false)])
    .with_projection_indices(Some(projection_index.clone()))
    .with_file_group(file_group)
    .build();

    let parquet_exec = DataSourceExec::from_data_source(file_scan_config.clone());

    let projection_exprs = build_projection_exprs(file_scan_config.projected_schema())
        .expect("Failed to build projection expressions");

    let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs, parquet_exec)
        .expect("Failed to create ProjectionExec"));
    let optimized_plan: Arc<dyn ExecutionPlan> = projection_exec.clone();
    let task_ctx = Arc::new(TaskContext::default());
    let stream = optimized_plan.execute(0, task_ctx)?;

    log_info!("[FLOW] Fetch phase execution stream created");
    Ok(get_cross_rt_stream(cpu_executor, stream))
}

fn is_aggs_query(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => {
            true
        },
        LogicalPlan::TableScan(_) => {
            // reached leaf
            false
        },
        // … handle other variants as needed …
        other => {
            let mut is_aggs = false;
            for child in other.inputs() {
                is_aggs = is_aggs || is_aggs_query(child);
                if is_aggs {
                    return is_aggs;
                }
            }
            is_aggs
        }
    }
}

pub fn create_projections(
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    schema: SchemaRef,
) -> Vec<String> {

    // Get all field names from schema
    let all_fields: Vec<String> = schema.fields().to_vec().iter().map(|f| f.name().to_string()).collect();

    match (include_fields.is_empty(), exclude_fields.is_empty()) {

        // includes empty, excludes empty → all fields
        (true, true) => all_fields.clone(),

        // includes non-empty → include only these fields
        (false, _) => include_fields
            .into_iter()
            .filter(|f| schema.field_with_name(f).is_ok())     // keep valid fields
            .collect(),

        // includes empty, excludes non-empty → remove excludes
        (true, false) => {
            let exclude_set: HashSet<String> =
                exclude_fields.into_iter().collect();

            all_fields
                .into_iter()
                .filter(|f| !exclude_set.contains(f))
                .collect()
        }
    }
}

/// Builds projection expressions that reconstruct absolute row IDs during fetch phase.
/// This function creates the physical expressions needed to convert file-relative row IDs
/// back to absolute row IDs by adding the row_base offset.
///
/// # Absolute Row ID Reconstruction
/// During the fetch phase, we read data directly from Parquet files, which contain
/// relative row IDs (0-based within each file). To maintain consistency with the
/// query phase results, we need to reconstruct the absolute row IDs using:
///
/// **absolute_row_id = relative_row_id + row_base**
///
/// Where:
/// - relative_row_id: The ___row_id field from the Parquet file (0-based)
/// - row_base: The partition column value representing this file's starting offset
/// - absolute_row_id: The globally unique row identifier
fn build_projection_exprs(new_schema: SchemaRef) -> std::result::Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError> {
    // Get column indices for the row ID reconstruction calculation
    let row_id_idx = new_schema.index_of(ROW_ID_FIELD_NAME).expect("Field ___row_id missing");
    let row_base_idx = new_schema.index_of(ROW_BASE_FIELD_NAME).expect("Field row_base missing");

    // Create the expression: ___row_id + row_base = absolute_row_id
    // This reconstructs the absolute row ID that was originally returned by the query phase
    let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_ID_FIELD_NAME, row_id_idx)),
        Operator::Plus,
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_BASE_FIELD_NAME, row_base_idx)),
    ));

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();

    let mut has_row_id = false;
    // Build projection expressions for all requested fields
    for field_name in new_schema.fields().to_vec() {
        if field_name.name() == ROW_ID_FIELD_NAME {
            // For ___row_id field, use the sum expression to get absolute row ID
            // This ensures the fetch phase returns the same absolute row IDs
            // that were originally identified in the query phase
            projection_exprs.push((sum_expr.clone(), field_name.name().clone()));
            has_row_id = true;
        } else if(field_name.name() != ROW_BASE_FIELD_NAME) {
            // For regular data fields, project them directly from the file
            // Skip row_base as it's only used for calculation, not output
            let idx = new_schema
                .index_of(&*field_name.name().clone())
                .unwrap_or_else(|_| panic!("Field {field_name} missing in schema"));
            projection_exprs.push((
                Arc::new(datafusion::physical_expr::expressions::Column::new(&*field_name.name(), idx)),
                field_name.name().clone(),
            ));
        }
    }

    // Ensure absolute row ID is always available in the output
    // This maintains consistency between query and fetch phases
    if !has_row_id {
        projection_exprs.push((sum_expr.clone(), ROW_ID_FIELD_NAME.parse().unwrap()));
    }
    Ok(projection_exprs)
}

async fn create_access_plans(
    row_ids: Vec<jlong>,
    files_metadata: Arc<Vec<CustomFileMeta>>,
) -> Result<Vec<ParquetAccessPlan>, DataFusionError> {
    let mut access_plans = Vec::new();
    let mut sorted_row_ids: Vec<i64> = row_ids.iter().map(|&id| id as i64).collect();
    sorted_row_ids.sort_unstable();

    for file_meta in files_metadata.iter() {
        let row_base = *file_meta.row_base;
        let total_row_groups = file_meta.row_group_row_counts.len();
        let mut access_plan = ParquetAccessPlan::new_all(total_row_groups);

        let file_total_rows: i64 = file_meta.row_group_row_counts.iter().map(|&x| x).sum();
        let file_end_row: i64 = row_base + file_total_rows;
        let file_row_ids: Vec<i64> = sorted_row_ids
            .iter()
            .copied()
            .filter(|&id| id >= row_base && id < file_end_row)
            .map(|id| id - row_base)
            .collect();

        if file_row_ids.is_empty() {
            for group_id in 0..total_row_groups {
                access_plan.skip(group_id);
            }
        } else {
            let mut cumulative_group_rows: Vec<i64> = Vec::with_capacity(total_row_groups + 1);
            cumulative_group_rows.push(0);
            let mut current_sum = 0;
            for &count in file_meta.row_group_row_counts.iter() {
                current_sum += count;
                cumulative_group_rows.push(current_sum);
            }

            let mut group_map: HashMap<usize, BTreeSet<i32>> = HashMap::new();
            for &row_id in &file_row_ids {
                let group_id = cumulative_group_rows
                    .windows(2)
                    .position(|window| row_id >= window[0] as i64 && row_id < window[1] as i64)
                    .unwrap();

                let relative_pos = row_id - cumulative_group_rows[group_id];
                group_map
                    .entry(group_id)
                    .or_default()
                    .insert(relative_pos as i32);
            }

            for group_id in 0..total_row_groups {
                let row_group_size = file_meta.row_group_row_counts[group_id] as usize;

                if let Some(group_row_ids) = group_map.get(&group_id) {
                    let mut relative_row_ids: Vec<usize> =
                        group_row_ids.iter().map(|&x| x as usize).collect();
                    relative_row_ids.sort_unstable();

                    if relative_row_ids.is_empty() {
                        access_plan.skip(group_id);
                    } else if relative_row_ids.len() == row_group_size {
                        access_plan.scan(group_id);
                    } else {
                        let mut selectors = Vec::new();
                        let mut current_pos = 0;
                        let mut i = 0;
                        while i < relative_row_ids.len() {
                            let target_pos = relative_row_ids[i];
                            if target_pos > current_pos {
                                selectors.push(RowSelector::skip(target_pos - current_pos));
                            }
                            let mut select_count = 1;
                            while i + 1 < relative_row_ids.len()
                                && relative_row_ids[i + 1] == relative_row_ids[i] + 1
                            {
                                select_count += 1;
                                i += 1;
                            }
                            selectors.push(RowSelector::select(select_count));
                            current_pos = relative_row_ids[i] + 1;
                            i += 1;
                        }
                        if current_pos < row_group_size {
                            selectors.push(RowSelector::skip(row_group_size - current_pos));
                        }
                        access_plan.set(group_id, RowGroupAccess::Selection(selectors.into()));
                    }
                } else {
                    access_plan.skip(group_id);
                }
            }
        }

        access_plans.push(access_plan);
    }

    Ok(access_plans)
}


/// Creates an Iceberg table provider from S3 Tables with partition filtering.
///
/// This is a convenience wrapper around s3_partition_downloader::create_iceberg_table_from_s3tables_partition
/// that provides the same interface as other create_iceberg_table_from_* functions.
///
/// # Arguments
/// * `table_bucket_arn` - S3 Tables bucket ARN
/// * `database_name` - Database/namespace name
/// * `table_name` - Table name
/// * `partition_column` - Partition column name (e.g., "shard_id")
/// * `partition_value` - Partition value to filter (e.g., "0")
/// * `state` - DataFusion session state
/// * `s3tables_options` - Optional S3 Tables configuration
///
/// # Returns
/// An Arc-wrapped TableProvider with partition filtering applied
async fn create_iceberg_table_from_s3tables_with_partition(
    table_bucket_arn: &str,
    database_name: &str,
    table_name: &str,
    partition_column: &str,
    partition_value: &str,
    state: &SessionState,
    s3tables_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    use crate::s3_partition_downloader::create_iceberg_table_from_s3tables_partition;

    create_iceberg_table_from_s3tables_partition(
        table_bucket_arn,
        database_name,
        table_name,
        partition_column,
        partition_value,
        state,
        s3tables_options,
    ).await
}
