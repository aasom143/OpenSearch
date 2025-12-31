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
import org.apache.iceberg.aws.glue.GlueCatalog;
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
        // Initialize Glue catalog
        this.catalog = new GlueCatalog();

        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", System.getProperty("iceberg.warehouse", "s3://srirasac-iceberg-test/os-warehouse"));
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        // Configure AWS region
        String region = System.getProperty("aws.region", "us-west-2");
        properties.put("glue.region", region);
        
        // Use URLConnection HTTP client instead of Apache client to avoid reflection issues
        properties.put("http-client.type", "urlconnection");

        catalog.initialize("glue_catalog", properties);

        logger.info("[Iceberg] Initialized Glue catalog in region: {} with urlconnection HTTP client", region);
    }

    public static IcebergManager getInstance() {
        return INSTANCE;
    }

    public Table getOrCreateTable(String indexName) {
        return tables.computeIfAbsent(indexName, this::createTable);
    }

    private Table createTable(String indexName) {
        TableIdentifier id = TableIdentifier.of("opensearch", indexName);

        try {
            Table table = catalog.loadTable(id);
            logger.info("[Iceberg] Loaded existing table: {}", indexName);
            return table;
        } catch (Exception e) {
            Schema schema = new Schema(
                Types.NestedField.required(1, "row_id", Types.LongType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())
            );

            // Create table with S3 location
            String warehouse = System.getProperty("iceberg.warehouse", "s3://opensearch-iceberg-warehouse/");
            String tableLocation = warehouse + "opensearch/" + indexName;

            Table table = catalog.buildTable(id, schema)
                .withLocation(tableLocation)
                .create();
            logger.info("[Iceberg] Created new table: {} at {}", indexName, tableLocation);
            return table;
        }
    }
}
