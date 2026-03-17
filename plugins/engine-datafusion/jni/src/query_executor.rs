/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use std::collections::{BTreeSet, HashMap, HashSet};
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
use arrow_schema::{DataType, Field, SchemaRef};
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
use datafusion::execution::context::SessionState;
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
use iceberg::{Catalog, CatalogBuilder, TableIdent, NamespaceIdent};
use iceberg_catalog_s3tables::{S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN};

/// Enum representing different table source types for query execution
#[derive(Debug, Clone)]
pub enum TableSource {
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

/// Creates an Iceberg table provider from AWS S3 Tables.
async fn create_iceberg_table_from_s3tables(
    table_bucket_arn: &str,
    database_name: &str,
    table_name: &str,
    s3tables_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let mut catalog_props = HashMap::new();
    catalog_props.insert(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(), table_bucket_arn.to_string());

    if let Some(opts) = s3tables_options {
        catalog_props.extend(opts);
    }

    let s3tables_catalog = S3TablesCatalogBuilder::default()
        .load("s3tables_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let namespace = NamespaceIdent::new(database_name.to_string());
    let table_ident = TableIdent::new(namespace, table_name.to_string());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let metadata_location = iceberg_table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();

    let factory = IcebergTableProviderFactory::new();
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

    Ok(provider)
}

/// Executes a query using DataFusion with cross-runtime streaming capabilities.
pub async fn execute_query_with_cross_rt_stream(
    table_source: TableSource,
    table_name: String,
    plan_bytes_vec: Vec<u8>,
    is_query_plan_explain_enabled: bool,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    // Set AWS profile for customer account - for Iceberg metadata and S3 Tables
    if let TableSource::IcebergS3Tables { .. } = &table_source {
        std::env::set_var("AWS_PROFILE", "customer-account");
        std::env::set_var("AWS_REGION", "us-west-2");
    }

    let runtimeEnv = &runtime.runtime_env;

    let runtime_env = RuntimeEnvBuilder::from_runtime_env(runtimeEnv)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_file_metadata_cache(Some(runtimeEnv.cache_manager.get_file_metadata_cache()))
                .with_files_statistics_cache(runtimeEnv.cache_manager.get_file_statistic_cache()),
        )
        .with_metadata_cache_limit(250 * 1024 * 1024)
        .build()?;

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
        TableSource::IcebergS3Tables { table_bucket_arn, database_name, s3tables_options, shard_id } => {
            create_iceberg_table_from_s3tables(table_bucket_arn, database_name, &table_name, s3tables_options.clone()).await?
        }
        TableSource::DownloadedPartition { local_dir } => {
            let local_url = format!("file://{}", local_dir);
            let table_path = ListingTableUrl::parse(&local_url)?;

            let file_format = Arc::new(ParquetFormat::default());
            let listing_options = DataFusionListingOptions::new(file_format)
                .with_file_extension(".parquet")
                .with_collect_stat(true);

            let inferred_schema = listing_options.infer_schema(&state, &table_path).await?;

            let listing_config = DataFusionListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(inferred_schema);

            Arc::new(DataFusionListingTable::try_new(listing_config)?)
        }
    };

    ctx.register_table(&table_name, provider.clone())?;

    // Decode substrait
    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => plan,
        Err(e) => {
            error!("Failed to decode Substrait plan: {}", e);
            return Err(DataFusionError::Execution(format!("Failed to decode Substrait: {}", e)));
        }
    };

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
            return Err(e);
        }
    };

    // Inject partition filter for S3 Tables with shard_id
    if let TableSource::IcebergS3Tables { shard_id: Some(shard), .. } = &table_source {
        if provider.schema().field_with_name("shard_id").is_ok() {
            let shard_value: i32 = shard.parse().unwrap_or(0);
            let filter_expr = col("shard_id").eq(lit(shard_value));
            logical_plan = inject_filter_at_table_scan(logical_plan, filter_expr)?;
        }
    }

    let is_aggregation_query = is_aggs_query(&logical_plan);

    if !is_aggregation_query {
        logical_plan = ProjectRowIdAnalyzer.analyze(logical_plan, ctx.state().config_options())?;
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

    if !is_aggregation_query {
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

    Ok(get_cross_rt_stream(cpu_executor, df_stream))
}

/// Helper function to inject a filter at the TableScan level of a logical plan.
fn inject_filter_at_table_scan(plan: LogicalPlan, filter_expr: Expr) -> Result<LogicalPlan, DataFusionError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    plan.transform(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                let filter_plan = LogicalPlan::Filter(datafusion_expr::Filter::try_new(
                    filter_expr.clone(),
                    Arc::new(LogicalPlan::TableScan(scan)),
                )?);
                Ok(Transformed::yes(filter_plan))
            }
            _ => Ok(Transformed::no(node)),
        }
    })
    .map(|transformed| transformed.data)
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

/// Executes the fetch phase of a two-phase query execution strategy.
pub async fn execute_fetch_phase(
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
    row_ids: Vec<jlong>,
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
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

    if(!projections.contains(&ROW_ID_FIELD_NAME.to_string())) {
        projection_index.push(parquet_schema.index_of(ROW_ID_FIELD_NAME).unwrap());
    }
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

    Ok(get_cross_rt_stream(cpu_executor, stream))
}

fn is_aggs_query(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::TableScan(_) => false,
        other => {
            for child in other.inputs() {
                if is_aggs_query(child) {
                    return true;
                }
            }
            false
        }
    }
}

pub fn create_projections(
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    schema: SchemaRef,
) -> Vec<String> {
    let all_fields: Vec<String> = schema.fields().to_vec().iter().map(|f| f.name().to_string()).collect();

    match (include_fields.is_empty(), exclude_fields.is_empty()) {
        (true, true) => all_fields.clone(),
        (false, _) => include_fields
            .into_iter()
            .filter(|f| schema.field_with_name(f).is_ok())
            .collect(),
        (true, false) => {
            let exclude_set: HashSet<String> = exclude_fields.into_iter().collect();
            all_fields
                .into_iter()
                .filter(|f| !exclude_set.contains(f))
                .collect()
        }
    }
}

fn build_projection_exprs(new_schema: SchemaRef) -> std::result::Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError> {
    let row_id_idx = new_schema.index_of(ROW_ID_FIELD_NAME).expect("Field ___row_id missing");
    let row_base_idx = new_schema.index_of(ROW_BASE_FIELD_NAME).expect("Field row_base missing");

    let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_ID_FIELD_NAME, row_id_idx)),
        Operator::Plus,
        Arc::new(datafusion::physical_expr::expressions::Column::new(ROW_BASE_FIELD_NAME, row_base_idx)),
    ));

    let mut projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let mut has_row_id = false;

    for field_name in new_schema.fields().to_vec() {
        if field_name.name() == ROW_ID_FIELD_NAME {
            projection_exprs.push((sum_expr.clone(), field_name.name().clone()));
            has_row_id = true;
        } else if(field_name.name() != ROW_BASE_FIELD_NAME) {
            let idx = new_schema
                .index_of(&*field_name.name().clone())
                .unwrap_or_else(|_| panic!("Field {field_name} missing in schema"));
            projection_exprs.push((
                Arc::new(datafusion::physical_expr::expressions::Column::new(&*field_name.name(), idx)),
                field_name.name().clone(),
            ));
        }
    }

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
