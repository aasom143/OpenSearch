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
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg tables for OpenSearch indices using AWS Glue catalog.
 * Auto-creates tables on first access.
 */
public class IcebergManager {
    private static final IcebergManager INSTANCE = new IcebergManager();
    private static final Logger logger = LogManager.getLogger(IcebergManager.class);

    private final GlueCatalog catalog;
    private final ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();

    private IcebergManager() {
        System.out.println("[Iceberg] Initializing Glue catalog with classloader fix...");
        
        this.catalog = new GlueCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", System.getProperty("iceberg.warehouse", "s3://srirasac-iceberg-test/os-warehouse"));
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        
        String region = System.getProperty("aws.region", "us-west-2");
        properties.put("glue.region", region);

        catalog.initialize("glue_catalog", properties);

        System.out.println("[Iceberg] Initialized Glue catalog in region: " + region);
    }

    public static IcebergManager getInstance() {
        return INSTANCE;
    }

    public Table getOrCreateTable(String indexName, org.apache.iceberg.Schema schema) {
        return tables.computeIfAbsent(indexName, name -> loadOrCreateTable(name, schema));
    }

    private Table loadOrCreateTable(String indexName, org.apache.iceberg.Schema schema) {
        // AWS Glue requires lowercase table names
        String tableName = indexName.toLowerCase().replace("-", "_");
        
        // Ensure namespace/database exists
        Namespace namespace = Namespace.of("opensearch");
        try {
            catalog.loadNamespaceMetadata(namespace);
            System.out.println("[Iceberg] Namespace 'opensearch' already exists");
        } catch (Exception e) {
            try {
                catalog.createNamespace(namespace);
                System.out.println("[Iceberg] Created namespace 'opensearch' in Glue");
            } catch (Exception createEx) {
                System.out.println("[Iceberg] Failed to create namespace: " + createEx.getMessage());
            }
        }
        
        TableIdentifier id = TableIdentifier.of("opensearch", tableName);

        try {
            // Always try to load existing table first
            Table table = catalog.loadTable(id);
            logger.info("[Iceberg] Loaded existing table: {} with {} fields", 
                       indexName, table.schema().columns().size());
            // TODO: Handle schema evolution if provided schema differs from existing
            return table;
        } catch (Exception e) {
            // Table doesn't exist
            if (schema == null) {
                logger.warn("[Iceberg] Table '{}' not found and no schema provided to create it", indexName);
                return null;
            }
            
            // Create new table with provided schema
            String warehouse = System.getProperty("iceberg.warehouse", "s3://srirasac-iceberg-test/os-warehouse/");
            String tableLocation = warehouse + "opensearch/" + indexName;

            logger.info("[Iceberg] Creating new table '{}' with {} fields", indexName, schema.columns().size());
            schema.columns().forEach(col -> 
                logger.debug("[Iceberg] Schema field: {} ({})", col.name(), col.type())
            );

            try {
                Table table = catalog.buildTable(id, schema)
                    .withLocation(tableLocation)
                    .create();
                    
                logger.info("[Iceberg] Successfully created table: {} at {}", indexName, tableLocation);
                return table;
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException alreadyExists) {
                // Race condition: Another shard created the table concurrently
                logger.info("[Iceberg] Table '{}' already exists (concurrent creation by another shard), loading it", indexName);
                try {
                    return catalog.loadTable(id);
                } catch (Exception loadEx) {
                    logger.error("[Iceberg] Failed to load table '{}' after concurrent creation", indexName, loadEx);
                    throw new RuntimeException("Failed to load table after concurrent creation", loadEx);
                }
            }
        }
    }
}
