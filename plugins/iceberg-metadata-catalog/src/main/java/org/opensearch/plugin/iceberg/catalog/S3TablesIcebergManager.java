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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg tables for OpenSearch indices using AWS S3 Tables.
 * S3 Tables provides native Iceberg table support with metadata stored in S3.
 * Auto-creates tables on first access.
 */
public class S3TablesIcebergManager {
    private static final S3TablesIcebergManager INSTANCE = new S3TablesIcebergManager();
    private static final Logger logger = LogManager.getLogger(S3TablesIcebergManager.class);

    private final RESTCatalog catalog;
    private final org.apache.iceberg.aws.s3.S3FileIO fileIO;
    private final ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();

    // Allow passing configuration from IcebergService
    private String bucketArn;
    private String region;

    private S3TablesIcebergManager() {
        System.out.println("[Iceberg S3Tables] ===== CONSTRUCTOR CALLED =====");
        System.out.println("[Iceberg S3Tables] Creating RESTCatalog instance");
        this.catalog = new RESTCatalog();
        System.out.println("[Iceberg S3Tables] RESTCatalog instance created");
        System.out.println("[Iceberg S3Tables] Creating properties map");

        Map<String, String> properties = new HashMap<>();

        // S3 Tables bucket ARN from environment or default
        // NOTE: This is called during static initialization, settings not available yet
        // We'll initialize catalog lazily when first accessed with proper settings
        this.bucketArn = "arn:aws:s3tables:us-west-2:339712837375:bucket/srirasac-test";  // Hardcode for now
        this.region = "us-west-2";

        String s3TablesBucketArn = this.bucketArn;

        System.out.println("[Iceberg S3Tables] Got bucket ARN: " + s3TablesBucketArn);

        System.out.println("[Iceberg S3Tables] Extracted region: " + region);

        // S3 Tables REST endpoint - CRITICAL: Must include /iceberg suffix
        String s3TablesEndpoint = String.format("https://s3tables.%s.amazonaws.com/iceberg", region);
        properties.put("uri", s3TablesEndpoint);

        // Warehouse - CRITICAL: Use bucket ARN directly, NOT an S3 path
        properties.put("warehouse", s3TablesBucketArn);

        // S3 FileIO configuration for data file access
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, String.format("https://s3.%s.amazonaws.com", region));
        properties.put("client.region", region);

        // AWS SigV4 authentication for S3 Tables REST endpoint
        properties.put("rest.sigv4-enabled", "true");
        properties.put("rest.signing-name", "s3tables");
        properties.put("rest.signing-region", region);

        // Let AWS SDK auto-discover credentials from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        // Don't explicitly set credentials-provider property - let it use default chain

        // Debug: Check if AWS credentials are available in environment
        String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("AWS_SESSION_TOKEN");
        System.out.println("[Iceberg S3Tables] AWS_ACCESS_KEY_ID present: " + (accessKeyId != null && !accessKeyId.isEmpty()));
        System.out.println("[Iceberg S3Tables] AWS_SECRET_ACCESS_KEY present: " + (secretKey != null && !secretKey.isEmpty()));
        System.out.println("[Iceberg S3Tables] AWS_SESSION_TOKEN present: " + (sessionToken != null && !sessionToken.isEmpty()));

        System.out.println("[Iceberg S3Tables] Properties configured: endpoint=" + s3TablesEndpoint);
        System.out.println("[Iceberg S3Tables] Properties: " + properties);
        System.out.println("[Iceberg S3Tables] About to call catalog.initialize()");

        // CRITICAL: Set thread context classloader so Iceberg's DynMethods can find AWS SDK classes
        // This is needed because plugin classloader isolation prevents dynamic class loading
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        System.out.println("[Iceberg S3Tables] Set context classloader to plugin classloader");

        try {
            // Initialize the catalog with timeout handling
            java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            java.util.concurrent.Future<?> future = executor.submit(() -> {
                try {
                    // Also set classloader in executor thread
                    Thread.currentThread().setContextClassLoader(S3TablesIcebergManager.class.getClassLoader());
                    System.out.println("[Iceberg S3Tables] Inside executor: calling catalog.initialize()");
                    catalog.initialize("s3tables_catalog", properties);
                    System.out.println("[Iceberg S3Tables] Inside executor: catalog.initialize() completed");
                } catch (Exception e) {
                    System.err.println("[Iceberg S3Tables] Exception in executor: " + e.getMessage());
                    e.printStackTrace(System.err);
                    throw new RuntimeException(e);
                }
            });

            // Wait up to 10 seconds for initialization
            future.get(10, java.util.concurrent.TimeUnit.SECONDS);
            System.out.println("[Iceberg S3Tables] catalog.initialize() returned successfully!");
            executor.shutdown();

            // Initialize S3FileIO for data file operations (with classloader still set)
            System.out.println("[Iceberg S3Tables] Initializing S3FileIO with customer-account profile");
            this.fileIO = new org.apache.iceberg.aws.s3.S3FileIO();
            Map<String, String> fileIOProperties = new HashMap<>();
            fileIOProperties.put(S3FileIOProperties.ENDPOINT, String.format("https://s3.%s.amazonaws.com", region));
            fileIOProperties.put("client.region", region);
            fileIOProperties.put("client.credentials-provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider");
            fileIOProperties.put("client.credentials-provider.profile-name", "customer-account");
            this.fileIO.initialize(fileIOProperties);
            System.out.println("[Iceberg S3Tables] S3FileIO initialized successfully with customer-account profile!");

        } catch (java.util.concurrent.TimeoutException e) {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
            System.err.println("[Iceberg S3Tables] TIMEOUT: catalog.initialize() took longer than 10 seconds!");
            System.err.println("[Iceberg S3Tables] This likely means:");
            System.err.println("[Iceberg S3Tables] 1. Network connectivity issue to S3 Tables endpoint: " + s3TablesEndpoint);
            System.err.println("[Iceberg S3Tables] 2. AWS credentials not configured properly");
            System.err.println("[Iceberg S3Tables] 3. Missing permissions for S3 Tables operations");
            throw new RuntimeException("S3 Tables catalog initialization timeout after 10 seconds", e);
        } catch (Exception e) {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
            System.err.println("[Iceberg S3Tables] ===== CATALOG INITIALIZATION FAILED =====");
            e.printStackTrace(System.err);
            throw new RuntimeException("Failed to initialize S3 Tables catalog", e);
        } finally {
            // Restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    public static S3TablesIcebergManager getInstance() {
        System.out.println("[Iceberg S3Tables] getInstance() called, about to return INSTANCE");
        return INSTANCE;
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
        Namespace namespace = Namespace.of("opensearch");

        try {
            catalog.loadNamespaceMetadata(namespace);
        } catch (Exception e) {
            try {
                catalog.createNamespace(namespace, new HashMap<>());
                logger.info("[Iceberg S3Tables] Created namespace 'opensearch' in customer account");
            } catch (Exception createEx) {
                logger.warn("[Iceberg S3Tables] Failed to create namespace: {}", createEx.getMessage());
            }
        }

        TableIdentifier tableId = TableIdentifier.of("opensearch", tableName);

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
        Namespace namespace = Namespace.of("opensearch");
        try {
            catalog.loadNamespaceMetadata(namespace);
            logger.info("[Iceberg S3Tables] Namespace 'opensearch' already exists");
        } catch (Exception e) {
            logger.info("[Iceberg S3Tables] Namespace 'opensearch' doesn't exist, creating it: {}", e.getMessage());
            try {
                // Create namespace in S3 Tables bucket
                // NOTE: S3 Tables doesn't support namespace properties, so pass empty map
                catalog.createNamespace(namespace, new HashMap<>());
                logger.info("[Iceberg S3Tables] Successfully created namespace 'opensearch'");
            } catch (Exception createEx) {
                logger.error("[Iceberg S3Tables] Failed to create namespace: {}", createEx.getMessage(), createEx);
                // Continue anyway - namespace might exist but loadNamespaceMetadata failed
            }
        }

        TableIdentifier tableId = TableIdentifier.of("opensearch", tableName);

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
            Namespace namespace = Namespace.of("opensearch");
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
        TableIdentifier tableId = TableIdentifier.of("opensearch", tableName.toLowerCase().replace("-", "_"));

        try {
            return customerCatalog.loadTable(tableId);
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Failed to load table for query: {}", tableName, e);
            throw new RuntimeException("Table not found: " + tableName, e);
        }
    }
}
