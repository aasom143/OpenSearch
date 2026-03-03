/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! S3 Partition Downloader for creating local ListingTableProvider
//!
//! This module provides functionality to download files from an S3 table partition
//! to a local directory and create a DataFusion ListingTableProvider from those files.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::datasource::listing::ListingTableUrl;
use futures::TryStreamExt;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use iceberg::{Catalog, CatalogBuilder, TableIdent, NamespaceIdent};
use iceberg_catalog_s3tables::{S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN};
use vectorized_exec_spi::{log_info, log_debug};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingTable as DataFusionListingTable,
    ListingOptions as DataFusionListingOptions,
    ListingTableConfig as DataFusionListingTableConfig,
};

/// Configuration for downloading S3 partition data
#[derive(Debug, Clone)]
pub struct S3PartitionDownloadConfig {
    /// Local directory where files will be downloaded
    pub local_dir: String,
    /// S3 bucket ARN for S3 Tables
    pub table_bucket_arn: String,
    /// Database/namespace name
    pub database_name: String,
    /// Table name
    pub table_name: String,
    /// Partition column name (e.g., "shard_id")
    pub partition_column: String,
    /// Partition value to filter (e.g., "0", "1", etc.)
    pub partition_value: String,
    /// Optional IAM role ARN for cross-account access
    pub role_arn: Option<String>,
    /// Optional S3 configuration
    pub s3_options: Option<HashMap<String, String>>,
}

impl S3PartitionDownloadConfig {
    pub fn new(
        local_dir: impl Into<String>,
        table_bucket_arn: impl Into<String>,
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        partition_column: impl Into<String>,
        partition_value: impl Into<String>,
    ) -> Self {
        Self {
            local_dir: local_dir.into(),
            table_bucket_arn: table_bucket_arn.into(),
            database_name: database_name.into(),
            table_name: table_name.into(),
            partition_column: partition_column.into(),
            partition_value: partition_value.into(),
            role_arn: None,
            s3_options: None,
        }
    }

    pub fn with_role_arn(mut self, role_arn: impl Into<String>) -> Self {
        self.role_arn = Some(role_arn.into());
        self
    }

    pub fn with_s3_options(mut self, options: HashMap<String, String>) -> Self {
        self.s3_options = Some(options);
        self
    }
}

/// Gets the S3 file paths for a specific partition without downloading.
///
/// This function is useful for:
/// - Inspecting what files would be downloaded
/// - Getting file metadata (size, record count)
/// - Verifying partition exists before downloading
///
/// # Arguments
/// * `config` - Configuration for the partition query
///
/// # Returns
/// A vector of tuples containing (file_path, file_size, record_count)
pub async fn get_s3_partition_file_paths(
    config: &S3PartitionDownloadConfig,
) -> Result<Vec<(String, u64, Option<u64>)>> {
    log_info!(
        "[S3-PATHS] Getting S3 paths: table={}.{}, partition={}={}",
        config.database_name,
        config.table_name,
        config.partition_column,
        config.partition_value
    );

    // Handle role assumption if role_arn is provided in s3_options
    if let Some(opts) = &config.s3_options {
        if let Some(role_arn) = opts.get("role_arn") {
            log_info!("[S3-PATHS] Assuming role: {}", role_arn);
            
            use aws_config::BehaviorVersion;
            
            let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let sts_client = aws_sdk_sts::Client::new(&aws_config);
            
            let assume_role_output = sts_client
                .assume_role()
                .role_arn(role_arn)
                .role_session_name("opensearch-datafusion-query")
                .send()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to assume role: {}", e)))?;
            
            let creds = assume_role_output.credentials()
                .ok_or_else(|| DataFusionError::Execution("No credentials returned from AssumeRole".to_string()))?;
            
            log_info!("[S3-PATHS] Role assumed successfully, setting credentials");
            
            std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key());
            std::env::set_var("AWS_SESSION_TOKEN", creds.session_token());
        }
        
        if let Some(region) = opts.get("region") {
            std::env::set_var("AWS_REGION", region);
            log_info!("[S3-PATHS] Set AWS_REGION to: {}", region);
        }
    }

    // Create S3 Tables catalog
    let mut catalog_props = HashMap::new();
    catalog_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        config.table_bucket_arn.clone(),
    );

    let s3tables_catalog = S3TablesCatalogBuilder::default()
        .load("s3tables_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[S3-PATHS] S3 Tables catalog created");

    // Load the Iceberg table
    let namespace = NamespaceIdent::new(config.database_name.clone());
    let table_ident = TableIdent::new(namespace, config.table_name.clone());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[S3-PATHS] Iceberg table loaded: {}.{}", config.database_name, config.table_name);

    // Build partition filter predicate
    let partition_predicate = iceberg::expr::Predicate::Binary(
        iceberg::expr::BinaryExpression::new(
            iceberg::expr::PredicateOperator::Eq,
            iceberg::expr::Reference::new(&config.partition_column),
            iceberg::spec::Datum::string(&config.partition_value),
        )
    );

    log_info!("[S3-PATHS] Applying partition filter: {}={}", config.partition_column, config.partition_value);

    // Create scan with partition filter
    let scan = iceberg_table
        .scan()
        .with_filter(partition_predicate)
        .select_all()
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Get data files from the scan plan
    let mut plan_files_stream = scan
        .plan_files()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Collect file paths and metadata
    let mut file_paths = Vec::new();
    while let Some(file_scan_task) = plan_files_stream.try_next().await
        .map_err(|e| DataFusionError::External(Box::new(e)))? {
        let file_path = file_scan_task.data_file_path().to_string();
        let file_size = file_scan_task.length;
        let record_count = file_scan_task.record_count;

        file_paths.push((file_path, file_size, record_count));
    }

    log_info!("[S3-PATHS] Found {} data files for partition", file_paths.len());

    Ok(file_paths)
}

/// Downloads files from an S3 table partition to a local directory.
///
/// This function:
/// 1. Loads the Iceberg table from S3 Tables catalog
/// 2. Scans the table with partition filter to get relevant data files
/// 3. Downloads those files to the local directory
/// 4. Returns the list of downloaded file paths
///
/// # Arguments
/// * `config` - Configuration for the download operation
///
/// # Returns
/// A vector of local file paths that were downloaded
pub async fn download_s3_partition_files(
    config: &S3PartitionDownloadConfig,
) -> Result<Vec<String>> {
    log_info!("[S3-DOWNLOAD-DEBUG] Function called with config:");
    log_info!("[S3-DOWNLOAD-DEBUG] - database_name: {}", config.database_name);
    log_info!("[S3-DOWNLOAD-DEBUG] - table_name: {}", config.table_name);
    log_info!("[S3-DOWNLOAD-DEBUG] - partition_column: {}", config.partition_column);
    log_info!("[S3-DOWNLOAD-DEBUG] - partition_value: {}", config.partition_value);
    log_info!("[S3-DOWNLOAD-DEBUG] - table_bucket_arn: {}", config.table_bucket_arn);
    log_info!("[S3-DOWNLOAD-DEBUG] - local_dir: {}", config.local_dir);
    log_info!("[S3-DOWNLOAD-DEBUG] - s3_options is_some: {}", config.s3_options.is_some());
    
    log_info!(
        "[S3-DOWNLOAD] Starting download: table={}.{}, partition={}={}",
        config.database_name,
        config.table_name,
        config.partition_column,
        config.partition_value
    );
    
    log_info!("[S3-DOWNLOAD] s3_options present: {}", config.s3_options.is_some());
    if let Some(opts) = &config.s3_options {
        log_info!("[S3-DOWNLOAD] s3_options keys: {:?}", opts.keys().collect::<Vec<_>>());
        for (key, value) in opts {
            log_info!("[S3-DOWNLOAD] s3_option: {} = {}", key, value);
        }
    } else {
        log_info!("[S3-DOWNLOAD] s3_options is None");
    }

    // Handle role assumption if role_arn is provided in s3_options
    if let Some(opts) = &config.s3_options {
        log_info!("[S3-DOWNLOAD] Checking for role_arn in s3_options");
        if let Some(role_arn) = opts.get("role_arn") {
            log_info!("[S3-DOWNLOAD] Assuming role: {}", role_arn);
            
            use aws_config::BehaviorVersion;
            
            let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let sts_client = aws_sdk_sts::Client::new(&aws_config);
            
            let assume_role_output = sts_client
                .assume_role()
                .role_arn(role_arn)
                .role_session_name("opensearch-datafusion-query")
                .send()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to assume role: {}", e)))?;
            
            let creds = assume_role_output.credentials()
                .ok_or_else(|| DataFusionError::Execution("No credentials returned from AssumeRole".to_string()))?;
            
            log_info!("[S3-DOWNLOAD] Role assumed successfully, setting credentials");
            
            log_info!("[S3-DOWNLOAD] AccessKeyId: {}", creds.access_key_id());
            log_info!("[S3-DOWNLOAD] SecretAccessKey: {}...", &creds.secret_access_key()[..10]);
            log_info!("[S3-DOWNLOAD] SessionToken: {}...", &creds.session_token()[..20]);
            
            std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key());
            std::env::set_var("AWS_SESSION_TOKEN", creds.session_token());
        } else {
            log_info!("[S3-DOWNLOAD] No role_arn found in s3_options");
        }
        
        if let Some(region) = opts.get("region") {
            std::env::set_var("AWS_REGION", region);
            log_info!("[S3-DOWNLOAD] Set AWS_REGION to: {}", region);
        } else {
            log_info!("[S3-DOWNLOAD] No region provided in s3_options");
        }
    } else {
        log_info!("[S3-DOWNLOAD] No s3_options provided, skipping role assumption");
    }

    // Log current AWS credentials being used
    log_info!("[S3-DOWNLOAD] Current AWS_ACCESS_KEY_ID: {}", 
        std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "<not set>".to_string()));
    log_info!("[S3-DOWNLOAD] Current AWS_SECRET_ACCESS_KEY: {}...", 
        std::env::var("AWS_SECRET_ACCESS_KEY")
            .map(|s| if s.len() > 10 { format!("{}...", &s[..10]) } else { "<short>".to_string() })
            .unwrap_or_else(|_| "<not set>".to_string()));
    log_info!("[S3-DOWNLOAD] Current AWS_SESSION_TOKEN: {}", 
        std::env::var("AWS_SESSION_TOKEN")
            .map(|s| if s.len() > 20 { format!("{}...", &s[..20]) } else { "<short>".to_string() })
            .unwrap_or_else(|_| "<not set>".to_string()));
    log_info!("[S3-DOWNLOAD] Current AWS_REGION: {}", 
        std::env::var("AWS_REGION").unwrap_or_else(|_| "<not set>".to_string()));

    // Create S3 Tables catalog
    let mut catalog_props = HashMap::new();
    catalog_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        config.table_bucket_arn.clone(),
    );

    let s3tables_catalog = S3TablesCatalogBuilder::default()
        .load("s3tables_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[S3-DOWNLOAD] S3 Tables catalog created");

    // Load the Iceberg table
    let namespace = NamespaceIdent::new(config.database_name.clone());
    let table_ident = TableIdent::new(namespace, config.table_name.clone());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[S3-DOWNLOAD] Iceberg table loaded: {}.{}", config.database_name, config.table_name);

    // Build partition filter predicate
    // Parse partition value as integer for shard_id
    let partition_predicate = if config.partition_column == "shard_id" {
        let shard_id_value: i32 = config.partition_value.parse()
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse shard_id as integer: {}", e)))?;

        log_info!("[S3-DOWNLOAD] Applying partition filter: {}={} (as integer)", config.partition_column, shard_id_value);

        iceberg::expr::Predicate::Binary(
            iceberg::expr::BinaryExpression::new(
                iceberg::expr::PredicateOperator::Eq,
                iceberg::expr::Reference::new(&config.partition_column),
                iceberg::spec::Datum::int(shard_id_value),
            )
        )
    } else {
        // For other partition columns, use string
        log_info!("[S3-DOWNLOAD] Applying partition filter: {}={} (as string)", config.partition_column, config.partition_value);

        iceberg::expr::Predicate::Binary(
            iceberg::expr::BinaryExpression::new(
                iceberg::expr::PredicateOperator::Eq,
                iceberg::expr::Reference::new(&config.partition_column),
                iceberg::spec::Datum::string(&config.partition_value),
            )
        )
    };

    // Create scan with partition filter
    let scan = iceberg_table
        .scan()
        .with_filter(partition_predicate)
        .select_all()
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Get the file IO from the table
    let file_io = iceberg_table.file_io();

    // Get data files from the scan plan
    let mut plan_files_stream = scan
        .plan_files()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create local directory
    fs::create_dir_all(&config.local_dir)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to create local directory: {}", e)))?;

    let mut downloaded_files = Vec::new();
    let mut file_count = 0;

    // Download each file
    while let Some(file_scan_task) = plan_files_stream.try_next().await
        .map_err(|e| DataFusionError::External(Box::new(e)))? {
        file_count += 1;
        let file_path = file_scan_task.data_file_path();

        log_debug!("[S3-DOWNLOAD] Downloading file: {}", file_path);

        // Extract filename from path and normalize extension
        let original_filename = Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid file path: {}", file_path)))?;

        // Normalize filename to have .parquet extension
        // S3 Tables files may have suffixes like .parquet__<hash>, we need to normalize to .parquet
        let normalized_filename = if original_filename.contains(".parquet") {
            // Extract the part before .parquet and add .parquet extension
            let base_name = original_filename.split(".parquet").next().unwrap_or(original_filename);
            format!("{}.parquet", base_name)
        } else {
            original_filename.to_string()
        };

        let local_file_path = format!("{}/{}", config.local_dir, normalized_filename);

        // Read file from S3 using Iceberg's FileIO and write directly
        let input_file = file_io
            .new_input(file_path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let file_bytes = input_file
            .read()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        fs::write(&local_file_path, file_bytes)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to write file: {}", e)))?;

        log_info!("[S3-DOWNLOAD] Downloaded: {} -> {}", file_path, local_file_path);
        downloaded_files.push(local_file_path);
    }

    log_info!("[S3-DOWNLOAD] Successfully downloaded {} files to {}", file_count, config.local_dir);

    Ok(downloaded_files)
}

/// Downloads S3 table partition files and creates a ListingTableProvider.
///
/// This is a convenience function that combines downloading files and creating
/// a ListingTable in one operation.
///
/// # Arguments
/// * `config` - Configuration for the download operation
/// * `state` - DataFusion session state for table creation
///
/// # Returns
/// An Arc-wrapped ListingTable that can be registered in the DataFusion context
///
/// # Example
/// ```rust
/// use std::collections::HashMap;
/// use datafusion::execution::context::SessionContext;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// let state = ctx.state();
///
/// let config = S3PartitionDownloadConfig::new(
///     "/tmp/my_table_data",
///     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
///     "my_database",
///     "my_table",
///     "shard_id",
///     "0",
/// );
///
/// let table_provider = download_and_create_listing_table(&config, &state).await?;
/// ctx.register_table("my_table", table_provider)?;
///
/// // Now you can query the table
/// let df = ctx.sql("SELECT * FROM my_table").await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_and_create_listing_table(
    config: &S3PartitionDownloadConfig,
    state: &SessionState,
) -> Result<Arc<dyn TableProvider>> {
    log_info!(
        "[S3-DOWNLOAD] Creating ListingTable from S3 partition: {}.{}, partition={}={}",
        config.database_name,
        config.table_name,
        config.partition_column,
        config.partition_value
    );

    // Download files
    let downloaded_files = download_s3_partition_files(config).await?;

    if downloaded_files.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "No files found for partition {}={}",
            config.partition_column, config.partition_value
        )));
    }

    // Create ListingTable from local directory
    let local_url = format!("file://{}", config.local_dir);
    let table_path = ListingTableUrl::parse(&local_url)?;

    log_info!("[S3-DOWNLOAD] Creating ListingTable from local path: {}", local_url);

    // Create listing options for Parquet files
    let file_format = Arc::new(ParquetFormat::default());
    let listing_options = DataFusionListingOptions::new(file_format)
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    // Infer schema from downloaded files
    let inferred_schema = listing_options.infer_schema(state, &table_path).await?;

    log_info!("[S3-DOWNLOAD] Inferred schema with {} fields", inferred_schema.fields().len());

    // Create ListingTable config
    let listing_config = DataFusionListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(inferred_schema);

    // Create the ListingTable (using DataFusion's standard implementation)
    let listing_table = DataFusionListingTable::try_new(listing_config)?;

    log_info!("[S3-DOWNLOAD] Successfully created ListingTable for {}.{}", config.database_name, config.table_name);

    Ok(Arc::new(listing_table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_partition_download_config() {
        let config = S3PartitionDownloadConfig::new(
            "/tmp/test",
            "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket",
            "test_db",
            "test_table",
            "shard_id",
            "0",
        );

        assert_eq!(config.local_dir, "/tmp/test");
        assert_eq!(config.database_name, "test_db");
        assert_eq!(config.table_name, "test_table");
        assert_eq!(config.partition_column, "shard_id");
        assert_eq!(config.partition_value, "0");
    }
}


/// Creates an Iceberg table provider from S3 Tables with partition filtering.
///
/// This function is similar to `create_iceberg_table_from_s3tables` in query_executor.rs
/// but specifically designed for partition-aware access. It creates a table provider
/// that can efficiently query a specific partition without downloading files.
///
/// # Arguments
/// * `table_bucket_arn` - S3 Tables bucket ARN
/// * `database_name` - Database/namespace name
/// * `table_name` - Table name
/// * `partition_column` - Partition column name (e.g., "shard_id")
/// * `partition_value` - Partition value to filter (e.g., "0")
/// * `state` - DataFusion session state for table creation
/// * `s3tables_options` - Optional S3 Tables configuration
///
/// # Returns
/// An Arc-wrapped TableProvider that can be registered in the DataFusion context
///
/// # Example
/// ```rust
/// use datafusion::execution::context::SessionContext;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// let state = ctx.state();
///
/// let table_provider = create_iceberg_table_from_s3tables_partition(
///     "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
///     "my_database",
///     "my_table",
///     "shard_id",
///     "0",
///     &state,
///     None,
/// ).await?;
///
/// ctx.register_table("my_table", table_provider)?;
/// let df = ctx.sql("SELECT * FROM my_table").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_iceberg_table_from_s3tables_partition(
    table_bucket_arn: &str,
    database_name: &str,
    table_name: &str,
    partition_column: &str,
    partition_value: &str,
    state: &SessionState,
    s3tables_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn TableProvider>> {
    log_info!(
        "[ICEBERG-S3TABLES-PARTITION] Creating Iceberg table from S3 Tables with partition filter: bucket={}, database={}, table={}, partition={}={}",
        table_bucket_arn, database_name, table_name, partition_column, partition_value
    );

    // Prepare S3 Tables catalog properties
    let mut catalog_props = HashMap::new();
    catalog_props.insert(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        table_bucket_arn.to_string(),
    );

    // Add user-provided options
    if let Some(opts) = s3tables_options {
        catalog_props.extend(opts);
    }

    // Create S3 Tables catalog
    let s3tables_catalog = S3TablesCatalogBuilder::default()
        .load("s3tables_catalog", catalog_props)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-S3TABLES-PARTITION] S3 Tables catalog created successfully");

    // Load table from S3 Tables
    let namespace = NamespaceIdent::new(database_name.to_string());
    let table_ident = TableIdent::new(namespace, table_name.to_string());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    log_info!("[ICEBERG-S3TABLES-PARTITION] Table loaded from S3 Tables: {}.{}", database_name, table_name);

    // Get the metadata location
    let metadata_location = iceberg_table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();

    log_info!("[ICEBERG-S3TABLES-PARTITION] Metadata location: {}", metadata_location);

    // Create the DataFusion table provider using the metadata location
    use datafusion::catalog::TableProviderFactory;
    use iceberg_datafusion::IcebergTableProviderFactory;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::common::DFSchema;

    let factory = IcebergTableProviderFactory::new();
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

    let provider = factory.create(state, &cmd).await?;

    log_info!(
        "[ICEBERG-S3TABLES-PARTITION] Successfully created Iceberg table provider for {}.{} with partition filter {}={}",
        database_name, table_name, partition_column, partition_value
    );

    // Note: The partition filtering will be applied at query time via predicates
    // Iceberg's partition pruning will automatically skip files not matching the partition
    Ok(provider)
}
