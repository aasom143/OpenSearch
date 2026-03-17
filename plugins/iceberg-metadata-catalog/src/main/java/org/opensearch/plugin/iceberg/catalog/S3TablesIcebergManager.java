/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.catalog;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg tables for OpenSearch indices using AWS S3 Tables.
 * S3 Tables provides native Iceberg table support with metadata stored in S3.
 * Auto-creates tables on first access.
 */
public class S3TablesIcebergManager {
    private static final Logger logger = LogManager.getLogger(S3TablesIcebergManager.class);

    private final RESTCatalog catalog;
    private final org.apache.iceberg.aws.s3.S3FileIO fileIO;
    private final ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();

    // Configuration from Settings
    private final String bucketArn;
    private final String region;
    private final String credentialsFilePath;
    private final String namespaceName;

    // Credentials loaded from file
    private final Map<String, String> fileCredentials;

    public S3TablesIcebergManager(org.opensearch.common.settings.Settings settings) {
        this.catalog = new RESTCatalog();

        Map<String, String> properties = new HashMap<>();

        // S3 Tables bucket ARN from environment or default
        // NOTE: This is called during static initialization, settings not available yet
        // We'll initialize catalog lazily when first accessed with proper settings
        // Read from Settings (no defaults - must be configured)

        this.credentialsFilePath = settings.get("iceberg.credentials.file", "/Users/guptasom/creds-iceberg/credentials.txt");
        this.namespaceName = settings.get("datafusion.iceberg.s3tables.namespace", "opensearch");
        this.bucketArn = settings.get("iceberg.s3tables.bucket.arn");
        this.region = settings.get("iceberg.aws.region");
        if (this.bucketArn == null || this.region == null) {
            throw new IllegalArgumentException("Missing required settings: iceberg.s3tables.bucket.arn and iceberg.aws.region");
        }

        // Load credentials from file
        this.fileCredentials = loadCredentialsFromFile(this.credentialsFilePath);
        logger.info("[Iceberg S3Tables] Loaded {} credentials from file: {}",
                   fileCredentials.size(), credentialsFilePath);

        String s3TablesBucketArn = this.bucketArn;



        // S3 Tables REST endpoint - CRITICAL: Must include /iceberg suffix
        String s3TablesEndpoint = String.format("https://s3tables.%s.amazonaws.com/iceberg", region);
        properties.put("uri", s3TablesEndpoint);

        // Warehouse - CRITICAL: Use bucket ARN directly, NOT an S3 path
        properties.put("warehouse", s3TablesBucketArn);

        // S3 FileIO configuration for data file access
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, String.format("https://s3.%s.amazonaws.com", region));
        properties.put("client.region", region);

        // NOTE: Default catalog initialization removed - not needed since we always use
        // customer-specific catalogs created on-demand via createCustomerCatalog()
        // This avoids startup failures when default account credentials are not available

        logger.info("[Iceberg S3Tables] S3TablesIcebergManager initialized (catalog will be created on-demand)");

        // Initialize S3FileIO without requiring catalog initialization
        // This FileIO is used for customer account operations
        this.fileIO = null;  // Will be created per-customer-account as needed
    }

    /**
     * Get the pre-initialized S3FileIO instance.
     * This FileIO is initialized with the correct classloader context.
     */
    public org.apache.iceberg.aws.s3.S3FileIO getFileIO() {
        return this.fileIO;
    }

    /**
     * Get or create an Iceberg table for the given index name.
     * S3 Tables automatically manages metadata and provides native Iceberg support.
     *
     * @param indexName OpenSearch index name
     * @param schema Iceberg schema for table creation (if table doesn't exist)
     * @return Iceberg Table instance, or null if table doesn't exist and no schema provided
     */
    public Table getOrCreateTable(String indexName, org.apache.iceberg.Schema schema) {
        return tables.computeIfAbsent(indexName, name -> loadOrCreateTable(name, schema));
    }

    /**
     * Get or create an Iceberg table in a customer account using role assumption.
     *
     * @param indexName OpenSearch index name
     * @param schema Iceberg schema for table creation
     * @param roleArn Customer IAM role ARN to assume
     * @param s3Bucket Customer S3 bucket name
     * @param region AWS region
     * @return Iceberg Table instance
     */
    public Table getOrCreateTableWithRole(String indexName, org.apache.iceberg.Schema schema,
                                          String roleArn, String s3Bucket, String region) {
        logger.info("[Iceberg S3Tables] Creating table in customer account: role={}, bucket={}, region={}",
                   roleArn, s3Bucket, region);

        RESTCatalog customerCatalog = createCustomerCatalog(roleArn, s3Bucket, region);
        return loadOrCreateTableWithCatalog(customerCatalog, indexName, schema);
    }

    /**
     * Create a RESTCatalog configured for customer account access.
     * Used for both write (sync) and read (query) operations.
     */
    public RESTCatalog createCustomerCatalog(String roleArn, String s3Bucket, String region) {
        // Assume customer role
        software.amazon.awssdk.services.sts.StsClient stsClient = software.amazon.awssdk.services.sts.StsClient.builder()
            .region(software.amazon.awssdk.regions.Region.of(region))
            .build();

        software.amazon.awssdk.services.sts.model.AssumeRoleRequest assumeRoleRequest =
            software.amazon.awssdk.services.sts.model.AssumeRoleRequest.builder()
                .roleArn(roleArn)
                .roleSessionName("opensearch-iceberg-query")
                .build();

        software.amazon.awssdk.services.sts.model.AssumeRoleResponse assumeRoleResponse =
            stsClient.assumeRole(assumeRoleRequest);

        software.amazon.awssdk.services.sts.model.Credentials credentials = assumeRoleResponse.credentials();

        logger.info("[Iceberg S3Tables] Successfully assumed role for query: {}", roleArn);

        // Create catalog with assumed credentials
        RESTCatalog customerCatalog = new RESTCatalog();
        Map<String, String> properties = new HashMap<>();

        String bucketArn = String.format("arn:aws:s3tables:%s:%s:bucket/%s",
                                        region, extractAccountId(roleArn), s3Bucket);
        String s3TablesEndpoint = String.format("https://s3tables.%s.amazonaws.com/iceberg", region);

        properties.put("uri", s3TablesEndpoint);
        properties.put("warehouse", bucketArn);
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, String.format("https://s3.%s.amazonaws.com", region));
        properties.put("client.region", region);
        properties.put("rest.sigv4-enabled", "true");
        properties.put("rest.signing-name", "s3tables");
        properties.put("rest.signing-region", region);

        // Use static credentials from assumed role
        properties.put("s3.access-key-id", credentials.accessKeyId());
        properties.put("s3.secret-access-key", credentials.secretAccessKey());
        properties.put("s3.session-token", credentials.sessionToken());

        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        try {
            customerCatalog.initialize("customer_catalog", properties);
            logger.info("[Iceberg S3Tables] Customer catalog initialized for queries");
            return customerCatalog;
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private String extractAccountId(String roleArn) {
        // Extract account ID from arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME
        String[] parts = roleArn.split(":");
        return parts.length > 4 ? parts[4] : "";
    }

    private Table loadOrCreateTableWithCatalog(RESTCatalog catalog, String indexName, org.apache.iceberg.Schema schema) {
        String tableName = indexName.toLowerCase().replace("-", "_");
        Namespace namespace = Namespace.of(namespaceName);

        try {
            catalog.loadNamespaceMetadata(namespace);
        } catch (Exception e) {
            try {
                catalog.createNamespace(namespace, new HashMap<>());
                logger.info("[Iceberg S3Tables] Created namespace '{}' in customer account", namespaceName);
            } catch (Exception createEx) {
                logger.warn("[Iceberg S3Tables] Failed to create namespace: {}", createEx.getMessage());
            }
        }

        TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

        try {
            return catalog.loadTable(tableId);
        } catch (Exception e) {
            logger.info("[Iceberg S3Tables] Creating new table in customer account: {}", indexName);

            org.apache.iceberg.PartitionSpec partitionSpec = null;
            if (schema.findField("index_uuid") != null && schema.findField("shard_id") != null) {
                partitionSpec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                    .identity("index_uuid")
                    .identity("shard_id")
                    .build();
            }

            if (partitionSpec != null) {
                return catalog.buildTable(tableId, schema)
                    .withPartitionSpec(partitionSpec)
                    .withProperty("write.format.default", "parquet")
                    .withProperty("write.parquet.compression-codec", "snappy")
                    .create();
            } else {
                return catalog.buildTable(tableId, schema)
                    .withProperty("write.format.default", "parquet")
                    .withProperty("write.parquet.compression-codec", "snappy")
                    .create();
            }
        }
    }

    private Table loadOrCreateTable(String indexName, org.apache.iceberg.Schema schema) {
        logger.info("[Iceberg S3Tables] loadOrCreateTable called: indexName={}, hasSchema={}",
                   indexName, (schema != null));

        // S3 Tables supports standard naming conventions
        String tableName = indexName.toLowerCase().replace("-", "_");

        // Ensure namespace exists in S3 Tables
        Namespace namespace = Namespace.of(namespaceName);
        try {
            catalog.loadNamespaceMetadata(namespace);
            logger.info("[Iceberg S3Tables] Namespace '{}' already exists", namespaceName);
        } catch (Exception e) {
            logger.info("[Iceberg S3Tables] Namespace '{}' doesn't exist, creating it: {}", namespaceName, e.getMessage());
            try {
                // Create namespace in S3 Tables bucket
                // NOTE: S3 Tables doesn't support namespace properties, so pass empty map
                catalog.createNamespace(namespace, new HashMap<>());
                logger.info("[Iceberg S3Tables] Successfully created namespace '{}'", namespaceName);
            } catch (Exception createEx) {
                logger.error("[Iceberg S3Tables] Failed to create namespace: {}", createEx.getMessage(), createEx);
                // Continue anyway - namespace might exist but loadNamespaceMetadata failed
            }
        }

        TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName);

        try {
            // Try to load existing table from S3 Tables
            logger.info("[Iceberg S3Tables] Attempting to load table: {}", tableId);
            Table table = catalog.loadTable(tableId);
            logger.info("[Iceberg S3Tables] Successfully loaded existing table: {} with {} fields",
                       indexName, table.schema().columns().size());
            // TODO: Handle schema evolution if provided schema differs from existing
            return table;
        } catch (Exception e) {
            // Table doesn't exist in S3 Tables
            logger.info("[Iceberg S3Tables] Table '{}' not found, will create: {}", indexName, e.getMessage());
            if (schema == null) {
                logger.warn("[Iceberg S3Tables] No schema provided to create table '{}'", indexName);
                return null;
            }

            // Create new table in S3 Tables
            // Note: S3 Tables manages table locations automatically within the bucket
            String tableLocation = null; // Let S3 Tables manage the location

            logger.info("[Iceberg] Creating new table in S3 Tables: '{}' with {} fields",
                       indexName, schema.columns().size());
            schema.columns().forEach(col ->
                logger.debug("[Iceberg] Schema field: {} ({})", col.name(), col.type())
            );

            try {
                // Create partition spec for hidden partitioning by index_uuid and shard_id
                org.apache.iceberg.PartitionSpec partitionSpec = null;
                try {
                    if (schema.findField("index_uuid") != null && schema.findField("shard_id") != null) {
                        partitionSpec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                            .identity("index_uuid")
                            .identity("shard_id")
                            .build();
                        logger.info("[Iceberg S3Tables] Created partition spec with index_uuid and shard_id");
                    } else {
                        logger.warn("[Iceberg S3Tables] Schema missing partition fields, creating unpartitioned table");
                    }
                } catch (Exception partitionEx) {
                    logger.warn("[Iceberg S3Tables] Failed to create partition spec: {}", partitionEx.getMessage());
                }


                // S3 Tables automatically manages table metadata
                logger.info("[Iceberg S3Tables] Creating table with ID: {}, schema fields: {}, partitioned: {}",
                           tableId, schema.columns().size(), (partitionSpec != null));

                // Build table with or without partition spec
                Table table;
                if (partitionSpec != null) {
                    table = catalog.buildTable(tableId, schema)
                        .withLocation(tableLocation)
                        .withPartitionSpec(partitionSpec)
                        .withProperty("write.format.default", "parquet")
                        .withProperty("write.parquet.compression-codec", "snappy")
                        .create();
                } else {
                    table = catalog.buildTable(tableId, schema)
                        .withLocation(tableLocation)
                        .withProperty("write.format.default", "parquet")
                        .withProperty("write.parquet.compression-codec", "snappy")
                        .create();
                }

                logger.info("[Iceberg S3Tables] ===== TABLE CREATED SUCCESSFULLY: {} (partitioned: {}) =====",
                           indexName, (partitionSpec != null));
                return table;
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException alreadyExists) {
                // Race condition: Another shard created the table concurrently
                logger.info("[Iceberg] Table '{}' already exists in S3 Tables (concurrent creation), loading it",
                           indexName);
                try {
                    return catalog.loadTable(tableId);
                } catch (Exception loadEx) {
                    logger.error("[Iceberg] Failed to load table '{}' from S3 Tables after concurrent creation",
                                indexName, loadEx);
                    throw new RuntimeException("Failed to load table from S3 Tables after concurrent creation", loadEx);
                }
            }
        }
    }

    /**
     * List all tables in the OpenSearch namespace.
     *
     * @return List of table identifiers
     */
    public java.util.List<TableIdentifier> listTables() {
        try {
            Namespace namespace = Namespace.of(namespaceName);
            return catalog.listTables(namespace);
        } catch (Exception e) {
            logger.error("[Iceberg] Failed to list tables from S3 Tables: {}", e.getMessage(), e);
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Load an Iceberg table from customer account for query execution.
     * Used by SQL/PPL plugin to execute queries against customer tables.
     *
     * @param tableName Table name (e.g., "guptasom_test_3_march_3")
     * @param roleArn Customer IAM role ARN
     * @param s3Bucket Customer S3 Tables bucket name
     * @param region AWS region
     * @return Iceberg Table instance
     */
    public Table loadTableForQuery(String tableName, String roleArn, String s3Bucket, String region) {
        logger.info("[Iceberg S3Tables] Loading table for query: table={}, role={}, bucket={}",
                   tableName, roleArn, s3Bucket);

        RESTCatalog customerCatalog = createCustomerCatalog(roleArn, s3Bucket, region);
        TableIdentifier tableId = TableIdentifier.of(namespaceName, tableName.toLowerCase().replace("-", "_"));

        try {
            return customerCatalog.loadTable(tableId);
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Failed to load table for query: {}", tableName, e);
            throw new RuntimeException("Table not found: " + tableName, e);
        }
    }

    /**
     * Get credentials loaded from file.
     * @return Map with keys: access_key, secret_key, session_token (if available)
     */
    public Map<String, String> getFileCredentials() {
        return new HashMap<>(fileCredentials);
    }

    /**
     * Load credentials from file.
     * Expected format:
     * aws_access_key_id=[access key]
     * aws_secret_access_key=[secret access key]
     * aws_session_token=[session token]
     *
     * @param filePath Path to credentials file
     * @return Map with keys: access_key, secret_key, session_token
     * @throws RuntimeException if file not found or required credentials missing
     */
    private static Map<String, String> loadCredentialsFromFile(String filePath) {
        Map<String, String> credentials = new HashMap<>();

        if (!Files.exists(Paths.get(filePath))) {
            String errorMsg = String.format("[Iceberg S3Tables] Credentials file not found: %s", filePath);
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue; // Skip empty lines and comments
                }

                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();

                    switch (key) {
                        case "aws_access_key_id":
                            credentials.put("access_key", value);
                            logger.info("[Iceberg S3Tables] Loaded access key from file");
                            break;
                        case "aws_secret_access_key":
                            credentials.put("secret_key", value);
                            logger.info("[Iceberg S3Tables] Loaded secret key from file");
                            break;
                        case "aws_session_token":
                            credentials.put("session_token", value);
                            logger.info("[Iceberg S3Tables] Loaded session token from file");
                            break;
                        default:
                            logger.debug("[Iceberg S3Tables] Ignoring unknown credential key: {}", key);
                    }
                }
            }

            // Validate required credentials are present
            if (!credentials.containsKey("access_key") || !credentials.containsKey("secret_key")) {
                String errorMsg = String.format(
                    "[Iceberg S3Tables] Missing required credentials in file: %s. " +
                    "File must contain aws_access_key_id and aws_secret_access_key",
                    filePath
                );
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }

            // DEBUG: Print full credential values to check for hidden characters
            logger.info("[DEBUG ICEBERG CREDS] Access Key: '{}'", credentials.get("access_key"));
            logger.info("[DEBUG ICEBERG CREDS] Secret Key: '{}'", credentials.get("secret_key"));
            if (credentials.containsKey("session_token")) {
                logger.info("[DEBUG ICEBERG CREDS] Session Token: '{}'", credentials.get("session_token"));
            }
            logger.info("[DEBUG ICEBERG CREDS] Access Key Length: {}", credentials.get("access_key").length());
            logger.info("[DEBUG ICEBERG CREDS] Secret Key Length: {}", credentials.get("secret_key").length());
            if (credentials.containsKey("session_token")) {
                logger.info("[DEBUG ICEBERG CREDS] Session Token Length: {}", credentials.get("session_token").length());
            }

            logger.info("[Iceberg S3Tables] Successfully loaded {} credential(s) from: {}",
                       credentials.size(), filePath);
        } catch (IOException e) {
            String errorMsg = String.format("[Iceberg S3Tables] Failed to read credentials file: %s", filePath);
            logger.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }

        return credentials;
    }
}
