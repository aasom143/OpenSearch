/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.iceberg;

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
    private final ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();

    private S3TablesIcebergManager() {
        System.out.println("[Iceberg S3Tables] ===== CONSTRUCTOR CALLED =====");
        System.out.println("[Iceberg S3Tables] Creating RESTCatalog instance");
        this.catalog = new RESTCatalog();
        System.out.println("[Iceberg S3Tables] RESTCatalog instance created");
        System.out.println("[Iceberg S3Tables] Creating properties map");

        Map<String, String> properties = new HashMap<>();
        
        // S3 Tables bucket ARN (format: arn:aws:s3tables:region:account-id:bucket/bucket-name)
        String s3TablesBucketArn = System.getProperty("iceberg.s3tables.bucket.arn", 
            "arn:aws:s3tables:us-west-2:123456789012:bucket/opensearch-iceberg-tables");
        
        System.out.println("[Iceberg S3Tables] Got bucket ARN: " + s3TablesBucketArn);
        
        // Extract region from ARN
        String[] arnParts = s3TablesBucketArn.split(":");
        String region = arnParts.length > 3 ? arnParts[3] : System.getProperty("aws.region", "us-west-2");
        
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
        
        System.out.println("[Iceberg S3Tables] Properties configured: endpoint=" + s3TablesEndpoint);
        System.out.println("[Iceberg S3Tables] About to call catalog.initialize()");
        
        try {
            // Initialize the catalog with timeout handling
            java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            java.util.concurrent.Future<?> future = executor.submit(() -> {
                try {
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
        } catch (java.util.concurrent.TimeoutException e) {
            System.err.println("[Iceberg S3Tables] TIMEOUT: catalog.initialize() took longer than 10 seconds!");
            System.err.println("[Iceberg S3Tables] This likely means:");
            System.err.println("[Iceberg S3Tables] 1. Network connectivity issue to S3 Tables endpoint: " + s3TablesEndpoint);
            System.err.println("[Iceberg S3Tables] 2. AWS credentials not configured properly");
            System.err.println("[Iceberg S3Tables] 3. Missing permissions for S3 Tables operations");
            throw new RuntimeException("S3 Tables catalog initialization timeout after 10 seconds", e);
        } catch (Exception e) {
            System.err.println("[Iceberg S3Tables] ===== CATALOG INITIALIZATION FAILED =====");
            e.printStackTrace(System.err);
            throw new RuntimeException("Failed to initialize S3 Tables catalog", e);
        }
    }

    public static S3TablesIcebergManager getInstance() {
        System.out.println("[Iceberg S3Tables] getInstance() called, about to return INSTANCE");
        return INSTANCE;
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
                // S3 Tables automatically manages table metadata
                logger.info("[Iceberg S3Tables] Creating table with ID: {}, schema fields: {}", 
                           tableId, schema.columns().size());
                Table table = catalog.buildTable(tableId, schema)
                    .withLocation(tableLocation)
                    .withProperty("write.format.default", "parquet")
                    .withProperty("write.parquet.compression-codec", "snappy")
                    .create();
                    
                logger.info("[Iceberg S3Tables] ===== TABLE CREATED SUCCESSFULLY: {} =====", indexName);
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
     * Useful for debugging and monitoring S3 Tables integration.
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
     * Drop a table from S3 Tables catalog.
     * This removes metadata but preserves data files in S3.
     * 
     * @param indexName OpenSearch index name
     * @return true if table was dropped, false otherwise
     */
    public boolean dropTable(String indexName) {
        String tableName = indexName.toLowerCase().replace("-", "_");
        TableIdentifier tableId = TableIdentifier.of("opensearch", tableName);
        
        try {
            boolean dropped = catalog.dropTable(tableId);
            if (dropped) {
                tables.remove(indexName);
                logger.info("[Iceberg] Dropped table from S3 Tables: {}", indexName);
            }
            return dropped;
        } catch (Exception e) {
            logger.error("[Iceberg] Failed to drop table from S3 Tables: {}", indexName, e);
            return false;
        }
    }

    /**
     * Refresh table metadata from S3 Tables.
     * Useful when external processes modify the table.
     * 
     * @param indexName OpenSearch index name
     */
    public void refreshTable(String indexName) {
        try {
            String tableName = indexName.toLowerCase().replace("-", "_");
            TableIdentifier tableId = TableIdentifier.of("opensearch", tableName);
            Table table = catalog.loadTable(tableId);
            tables.put(indexName, table);
            logger.debug("[Iceberg] Refreshed table metadata from S3 Tables: {}", indexName);
        } catch (Exception e) {
            logger.warn("[Iceberg] Failed to refresh table from S3 Tables: {}", indexName, e);
        }
    }
}
