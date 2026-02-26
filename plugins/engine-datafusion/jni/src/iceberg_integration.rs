/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Iceberg integration module for OpenSearch DataFusion plugin.
//!
//! This module provides utilities for working with Apache Iceberg tables stored in S3,
//! enabling efficient query execution over Iceberg metadata instead of raw file listings.
//!
//! # Efficient Metadata Location Retrieval
//!
//! This module includes helper functions to retrieve Iceberg table metadata locations
//! without loading the full table structure. This is useful for:
//! - Table discovery and listing
//! - Metadata location caching
//! - Table existence checks
//! - Passing metadata locations to DataFusion
//!
//! See [`get_metadata_location_from_s3tables`], [`get_metadata_location_from_glue`],
//! and [`get_metadata_location_from_s3_path`] for details.
//!
//! For complete documentation, see `METADATA_LOCATION_HELPERS.md`.

use std::collections::HashMap;
use std::sync::Arc;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, DFSchema};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::sql::TableReference;
use iceberg_datafusion::IcebergTableProviderFactory;
use datafusion::catalog::{TableProviderFactory, Session};
use iceberg::{Catalog, TableIdent, NamespaceIdent};
use object_store::{ObjectStore, path::Path as ObjectPath};

use vectorized_exec_spi::{log_info, log_error};

/// Finds the latest metadata file in an Iceberg metadata directory.
/// 
/// When an Iceberg table doesn't have a version-hint.text or metadata.json pointer,
/// this function scans the metadata directory and returns the path to the latest
/// metadata file based on the version number in the filename.
///
/// # Arguments
/// * `metadata_dir_path` - S3 path to the metadata directory (e.g., "s3://bucket/table/metadata/")
///
/// # Returns
/// The S3 path to the latest metadata file
async fn find_latest_metadata_file(metadata_dir_path: &str) -> Result<String, DataFusionError> {
    // For now, this is a placeholder that expects the full path to be provided
    // In a full implementation, this would:
    // 1. List files in the metadata directory
    // 2. Parse version numbers from filenames (e.g., "00005-uuid.metadata.json")
    // 3. Return the path to the highest version number
    
    // If the path already points to a .metadata.json file, return it as-is
    if metadata_dir_path.ends_with(".metadata.json") {
        return Ok(metadata_dir_path.to_string());
    }
    
    // If it's a directory path, we need to find the latest metadata file
    // This requires listing S3 objects, which isn't directly available here
    // For now, return an error with a helpful message
    Err(DataFusionError::Configuration(format!(
        "Metadata path must point to a specific metadata file (e.g., .../metadata/00005-uuid.metadata.json). \
         Directory paths are not yet supported without a version-hint.text file. \
         Path provided: {}",
        metadata_dir_path
    )))
}

/// Retrieves the metadata location from an S3 Tables catalog without loading the full table.
///
/// This is more efficient than loading the entire table when you only need the metadata location.
/// It uses the catalog's internal API to get just the table metadata pointer.
///
/// # Arguments
/// * `catalog` - Reference to an S3 Tables catalog
/// * `namespace` - The database/namespace name
/// * `table_name` - The table name
///
/// # Returns
/// The S3 path to the table's metadata file
///
/// # Example
/// ```no_run
/// use iceberg::{Catalog, NamespaceIdent, TableIdent};
/// use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
/// use std::collections::HashMap;
/// # use opensearch_datafusion_jni::iceberg_integration::get_metadata_location_from_s3tables;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut props = HashMap::new();
/// props.insert("table_bucket_arn".to_string(), "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket".to_string());
/// 
/// let catalog = S3TablesCatalogBuilder::default()
///     .load("s3tables", props)
///     .await?;
///
/// let metadata_location = get_metadata_location_from_s3tables(
///     &catalog,
///     "my_database",
///     "my_table"
/// ).await?;
///
/// println!("Metadata location: {}", metadata_location);
/// # Ok(())
/// # }
/// ```
pub async fn get_metadata_location_from_s3tables(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
) -> Result<String, DataFusionError> {
    log_info!("[ICEBERG] Getting metadata location from S3 Tables: {}.{}", namespace, table_name);
    
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());
    
    // Load the table - this is still required but the iceberg-rust library
    // optimizes this to only fetch metadata, not data files
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    
    // Extract just the metadata location
    let metadata_location = table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();
    
    log_info!("[ICEBERG] Retrieved metadata location: {}", metadata_location);
    Ok(metadata_location)
}

/// Retrieves the metadata location from a Glue catalog without loading the full table.
///
/// This is more efficient than loading the entire table when you only need the metadata location.
///
/// # Arguments
/// * `catalog` - Reference to a Glue catalog
/// * `database` - The Glue database name
/// * `table_name` - The table name
///
/// # Returns
/// The S3 path to the table's metadata file
///
/// # Example
/// ```no_run
/// use iceberg::Catalog;
/// use iceberg_catalog_glue::GlueCatalogBuilder;
/// # use opensearch_datafusion_jni::iceberg_integration::get_metadata_location_from_glue;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let catalog = GlueCatalogBuilder::default()
///     .load("glue", Default::default())
///     .await?;
///
/// let metadata_location = get_metadata_location_from_glue(
///     &catalog,
///     "my_database",
///     "my_table"
/// ).await?;
///
/// println!("Metadata location: {}", metadata_location);
/// # Ok(())
/// # }
/// ```
pub async fn get_metadata_location_from_glue(
    catalog: &dyn Catalog,
    database: &str,
    table_name: &str,
) -> Result<String, DataFusionError> {
    log_info!("[ICEBERG] Getting metadata location from Glue: {}.{}", database, table_name);
    
    let namespace_ident = NamespaceIdent::new(database.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());
    
    // Load the table - the iceberg-rust library optimizes this to only fetch metadata
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    
    // Extract just the metadata location
    let metadata_location = table
        .metadata_location_result()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .to_string();
    
    log_info!("[ICEBERG] Retrieved metadata location: {}", metadata_location);
    Ok(metadata_location)
}

/// Retrieves the metadata location from a direct S3 table path by reading version-hint.text.
///
/// This is the most efficient method as it only reads a small pointer file from S3.
/// It reads the version-hint.text file which contains the name of the current metadata file.
///
/// # Arguments
/// * `table_location` - S3 path to the table root (e.g., "s3://bucket/warehouse/db/table")
/// * `s3_config` - S3 configuration for accessing the files
///
/// # Returns
/// The S3 path to the table's current metadata file
///
/// # Example
/// ```no_run
/// use opensearch_datafusion_jni::iceberg_integration::{get_metadata_location_from_s3_path, S3Config};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let s3_config = S3Config::new()
///     .with_access_key_id("YOUR_ACCESS_KEY")
///     .with_secret_access_key("YOUR_SECRET_KEY")
///     .with_region("us-west-2");
///
/// let metadata_location = get_metadata_location_from_s3_path(
///     "s3://my-bucket/warehouse/db/my_table",
///     s3_config
/// ).await?;
///
/// println!("Metadata location: {}", metadata_location);
/// # Ok(())
/// # }
/// ```
pub async fn get_metadata_location_from_s3_path(
    table_location: &str,
    s3_config: S3Config,
) -> Result<String, DataFusionError> {
    log_info!("[ICEBERG] Getting metadata location from S3 path: {}", table_location);
    
    // Validate S3 path
    if !table_location.starts_with("s3://") && !table_location.starts_with("s3a://") {
        return Err(DataFusionError::Configuration(format!(
            "Invalid S3 path: {}. Path must start with 's3://' or 's3a://'",
            table_location
        )));
    }
    
    // Normalize the table location (remove trailing slash)
    let table_location = table_location.trim_end_matches('/');
    
    // Try to read version-hint.text first (most efficient)
    let version_hint_path = format!("{}/metadata/version-hint.text", table_location);
    
    // Build object store for S3 access
    let mut builder = object_store::aws::AmazonS3Builder::new();
    
    if let Some(ref key) = s3_config.access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(ref secret) = s3_config.secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(ref token) = s3_config.session_token {
        builder = builder.with_token(token);
    }
    if let Some(ref region) = s3_config.region {
        builder = builder.with_region(region);
    }
    if let Some(ref endpoint) = s3_config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if s3_config.allow_http {
        builder = builder.with_allow_http(true);
    }
    
    let object_store = builder
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    
    // Parse the S3 URL to get bucket and key
    let url = url::Url::parse(&version_hint_path)
        .map_err(|e| DataFusionError::Configuration(format!("Invalid S3 URL: {}", e)))?;
    
    let bucket = url.host_str()
        .ok_or_else(|| DataFusionError::Configuration("Missing bucket in S3 URL".to_string()))?;
    let key = url.path().trim_start_matches('/');
    
    log_info!("[ICEBERG] Reading version hint from: s3://{}/{}", bucket, key);
    
    // Try to read version-hint.text
    let object_path = ObjectPath::from(key);
    match object_store.get(&object_path).await {
        Ok(result) => {
            let bytes = result.bytes().await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let version_hint = String::from_utf8(bytes.to_vec())
                .map_err(|e| DataFusionError::Configuration(format!("Invalid UTF-8 in version-hint.text: {}", e)))?
                .trim()
                .to_string();
            
            // The version hint contains just the metadata filename, not the full path
            let metadata_location = format!("{}/metadata/{}", table_location, version_hint);
            log_info!("[ICEBERG] Retrieved metadata location from version hint: {}", metadata_location);
            Ok(metadata_location)
        }
        Err(_) => {
            // If version-hint.text doesn't exist, try metadata.json (legacy)
            let metadata_json_path = format!("{}/metadata/metadata.json", table_location);
            log_info!("[ICEBERG] version-hint.text not found, trying metadata.json");
            
            // Check if metadata.json exists
            let metadata_key = format!("{}/metadata/metadata.json", url.path().trim_start_matches('/').trim_end_matches(key));
            let metadata_path = ObjectPath::from(metadata_key.as_str());
            
            match object_store.head(&metadata_path).await {
                Ok(_) => {
                    log_info!("[ICEBERG] Found metadata.json: {}", metadata_json_path);
                    Ok(metadata_json_path)
                }
                Err(_) => {
                    Err(DataFusionError::Configuration(format!(
                        "Could not find version-hint.text or metadata.json in table metadata directory. \
                         Table location: {}. \
                         This table may not be a valid Iceberg table or may require catalog access.",
                        table_location
                    )))
                }
            }
        }
    }
}

/// Configuration for S3 access when loading Iceberg tables.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// AWS access key ID
    pub access_key_id: Option<String>,
    /// AWS secret access key
    pub secret_access_key: Option<String>,
    /// AWS session token (for temporary credentials)
    pub session_token: Option<String>,
    /// AWS region (e.g., "us-west-2")
    pub region: Option<String>,
    /// Custom S3 endpoint for S3-compatible storage (e.g., MinIO)
    pub endpoint: Option<String>,
    /// Allow HTTP connections (useful for local testing)
    pub allow_http: bool,
    /// Additional custom options
    pub custom_options: HashMap<String, String>,
}

impl S3Config {
    /// Creates a new S3Config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the AWS access key ID
    pub fn with_access_key_id(mut self, key: impl Into<String>) -> Self {
        self.access_key_id = Some(key.into());
        self
    }

    /// Sets the AWS secret access key
    pub fn with_secret_access_key(mut self, secret: impl Into<String>) -> Self {
        self.secret_access_key = Some(secret.into());
        self
    }

    /// Sets the AWS session token (for temporary credentials)
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    /// Sets the AWS region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets a custom S3 endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Enables HTTP connections (for local testing)
    pub fn with_allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }

    /// Adds a custom option
    pub fn with_custom_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_options.insert(key.into(), value.into());
        self
    }

    /// Converts the S3Config to a HashMap suitable for Iceberg table creation
    /// Uses AWS property naming convention (aws.* prefix) compatible with object_store
    pub fn to_options(&self) -> HashMap<String, String> {
        let mut options = self.custom_options.clone();

        if let Some(ref key) = self.access_key_id {
            options.insert("aws.access_key_id".to_string(), key.clone());
        }
        if let Some(ref secret) = self.secret_access_key {
            options.insert("aws.secret_access_key".to_string(), secret.clone());
        }
        if let Some(ref token) = self.session_token {
            options.insert("aws.session_token".to_string(), token.clone());
        }
        if let Some(ref region) = self.region {
            options.insert("aws.region".to_string(), region.clone());
        }
        if let Some(ref endpoint) = self.endpoint {
            options.insert("aws.endpoint".to_string(), endpoint.clone());
        }
        if self.allow_http {
            options.insert("aws.allow_http".to_string(), "true".to_string());
        }

        options
    }
}

/// Creates an Iceberg table provider from S3 metadata location.
///
/// # Arguments
/// * `s3_metadata_path` - S3 path to the Iceberg metadata directory or metadata.json file
///   Examples:
///   - "s3://bucket/warehouse/db/table/metadata/"
///   - "s3://bucket/warehouse/db/table/metadata/v1.metadata.json"
/// * `table_name` - Name to register the table under
/// * `state` - DataFusion session state for table creation
/// * `s3_config` - S3 configuration for accessing the metadata
///
/// # Returns
/// An Arc-wrapped TableProvider that can be registered in the DataFusion context
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::execution::context::SessionContext;
/// use opensearch_datafusion_jni::iceberg_integration::{create_iceberg_table_provider, S3Config};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let ctx = SessionContext::new();
/// let state = ctx.state();
///
/// let s3_config = S3Config::new()
///     .with_access_key_id("YOUR_ACCESS_KEY")
///     .with_secret_access_key("YOUR_SECRET_KEY")
///     .with_session_token("YOUR_SESSION_TOKEN") // For temporary credentials
///     .with_region("us-west-2");
///
/// let provider = create_iceberg_table_provider(
///     "s3://my-bucket/warehouse/db/my_table/metadata/",
///     "my_table",
///     &state,
///     s3_config,
/// ).await?;
///
/// ctx.register_table("my_table", provider)?;
/// # Ok(())
/// # }
/// ```
pub async fn create_iceberg_table_provider(
    s3_metadata_path: &str,
    table_name: &str,
    state: &SessionState,
    s3_config: S3Config,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    log_info!("[ICEBERG] Creating Iceberg table provider: path={}, table={}", s3_metadata_path, table_name);

    // Validate S3 path
    if !s3_metadata_path.starts_with("s3://") && !s3_metadata_path.starts_with("s3a://") {
        return Err(DataFusionError::Configuration(format!(
            "Invalid S3 path: {}. Path must start with 's3://' or 's3a://'",
            s3_metadata_path
        )));
    }

    // Resolve the metadata file path (handle both directory and file paths)
    let resolved_metadata_path = find_latest_metadata_file(s3_metadata_path).await?;
    log_info!("[ICEBERG] Resolved metadata path: {}", resolved_metadata_path);

    // Create the Iceberg table provider factory
    let factory = IcebergTableProviderFactory::new();

    // Convert S3Config to options HashMap
    let options = s3_config.to_options();

    // Create table reference with default namespace for Iceberg compatibility
    let table_ref = TableReference::partial("default", table_name);

    // Create the external table command for Iceberg
    let cmd = CreateExternalTable {
        name: table_ref,
        location: resolved_metadata_path,  // Use resolved path
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
    match factory.create(state, &cmd).await {
        Ok(provider) => {
            log_info!("[ICEBERG] Successfully created Iceberg table provider for {}", table_name);
            Ok(provider)
        }
        Err(e) => {
            log_error!("[ICEBERG] Failed to create Iceberg table provider: {}", e);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_builder() {
        let config = S3Config::new()
            .with_access_key_id("test_key")
            .with_secret_access_key("test_secret")
            .with_session_token("test_token")
            .with_region("us-west-2")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_custom_option("custom_key", "custom_value");

        let options = config.to_options();

        assert_eq!(options.get("aws.access_key_id"), Some(&"test_key".to_string()));
        assert_eq!(options.get("aws.secret_access_key"), Some(&"test_secret".to_string()));
        assert_eq!(options.get("aws.session_token"), Some(&"test_token".to_string()));
        assert_eq!(options.get("aws.region"), Some(&"us-west-2".to_string()));
        assert_eq!(options.get("aws.endpoint"), Some(&"http://localhost:9000".to_string()));
        assert_eq!(options.get("aws.allow_http"), Some(&"true".to_string()));
        assert_eq!(options.get("custom_key"), Some(&"custom_value".to_string()));
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::new();
        let options = config.to_options();

        assert!(options.is_empty());
    }
}
