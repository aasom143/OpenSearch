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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use datafusion::common::{DataFusionError, Result};
use futures::TryStreamExt;
use tokio::fs;
use iceberg::{Catalog, CatalogBuilder, TableIdent, NamespaceIdent};
use iceberg_catalog_s3tables::{S3TablesCatalogBuilder, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN};

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

/// Downloads files from an S3 table partition to a local directory.
///
/// This function:
/// 1. Loads the Iceberg table from S3 Tables catalog
/// 2. Scans the table with partition filter to get relevant data files
/// 3. Downloads those files to the local directory
/// 4. Returns the list of downloaded file paths
pub async fn download_s3_partition_files(
    config: &S3PartitionDownloadConfig,
) -> Result<Vec<String>> {
    // Handle role assumption if role_arn is provided in s3_options
    if let Some(opts) = &config.s3_options {
        if let Some(role_arn) = opts.get("role_arn") {
            use aws_config::BehaviorVersion;

            let region = opts.get("region").map(|r| aws_sdk_sts::config::Region::new(r.clone()));
            let sts_config_builder = aws_config::defaults(BehaviorVersion::latest());
            let sts_config = if let (Some(access_key), Some(secret_key)) =
                (opts.get("aws_access_key_id"), opts.get("aws_secret_access_key")) {
                let creds = if let Some(session_token) = opts.get("aws_session_token") {
                    aws_sdk_sts::config::Credentials::new(
                        access_key, secret_key, Some(session_token.to_string()), None, "file-credentials"
                    )
                } else {
                    aws_sdk_sts::config::Credentials::new(
                        access_key, secret_key, None, None, "file-credentials"
                    )
                };
                let mut builder = sts_config_builder.credentials_provider(creds);
                if let Some(ref r) = region {
                    builder = builder.region(r.clone());
                }
                builder.load().await
            } else {
                sts_config_builder.load().await
            };

            let sts_client = aws_sdk_sts::Client::new(&sts_config);

            let assume_role_output = sts_client
                .assume_role()
                .role_arn(role_arn)
                .role_session_name("opensearch-datafusion-query")
                .send()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to assume role: {:?}", e)))?;

            let creds = assume_role_output.credentials()
                .ok_or_else(|| DataFusionError::Execution("No credentials returned from AssumeRole".to_string()))?;

            std::env::set_var("AWS_ACCESS_KEY_ID", creds.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", creds.secret_access_key());
            std::env::set_var("AWS_SESSION_TOKEN", creds.session_token());
        }

        if let Some(region) = opts.get("region") {
            std::env::set_var("AWS_REGION", region);
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

    // Load the Iceberg table
    let namespace = NamespaceIdent::new(config.database_name.clone());
    let table_ident = TableIdent::new(namespace, config.table_name.clone());

    let iceberg_table = s3tables_catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Build partition filter predicate
    let partition_predicate = if config.partition_column == "shard_id" {
        let shard_id_value: i32 = config.partition_value.parse()
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse shard_id as integer: {}", e)))?;

        iceberg::expr::Predicate::Binary(
            iceberg::expr::BinaryExpression::new(
                iceberg::expr::PredicateOperator::Eq,
                iceberg::expr::Reference::new(&config.partition_column),
                iceberg::spec::Datum::int(shard_id_value),
            )
        )
    } else {
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

    let file_io = iceberg_table.file_io();

    let mut plan_files_stream = scan
        .plan_files()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create local directory
    fs::create_dir_all(&config.local_dir)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to create local directory: {}", e)))?;

    let mut downloaded_files = Vec::new();

    // Download each file
    while let Some(file_scan_task) = plan_files_stream.try_next().await
        .map_err(|e| DataFusionError::External(Box::new(e)))? {
        let file_path = file_scan_task.data_file_path();

        let original_filename = Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| DataFusionError::Execution(format!("Invalid file path: {}", file_path)))?;

        // Normalize filename: S3 Tables files may have suffixes like .parquet__<hash>
        let normalized_filename = if original_filename.contains(".parquet") {
            let base_name = original_filename.split(".parquet").next().unwrap_or(original_filename);
            format!("{}.parquet", base_name)
        } else {
            original_filename.to_string()
        };

        let local_file_path = format!("{}/{}", config.local_dir, normalized_filename);

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

        downloaded_files.push(local_file_path);
    }

    Ok(downloaded_files)
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
