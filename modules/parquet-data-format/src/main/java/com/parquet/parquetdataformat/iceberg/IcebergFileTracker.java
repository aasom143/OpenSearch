/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tracks Parquet files written during flush operations and commits them
 * to Iceberg during refresh operations.
 */
public class IcebergFileTracker {
    private static final Logger logger = LogManager.getLogger(IcebergFileTracker.class);
    private final org.apache.iceberg.Schema icebergSchema;
    
    public IcebergFileTracker(org.apache.iceberg.Schema icebergSchema) {
        this.icebergSchema = icebergSchema;
        logger.info("[Iceberg] FileTracker initialized with schema containing {} fields", 
                   icebergSchema.columns().size());
    }
    
    /**
     * Commit specific S3 files with actual sizes to Iceberg for a given index.
     * 
     * @param indexName The index name to use for Iceberg table identification
     * @param s3PathsWithSizes Map of S3 paths to their actual file sizes in bytes
     */
    public synchronized void commitFilesWithSizes(String indexName, java.util.Map<String, Long> s3PathsWithSizes) {
        if (s3PathsWithSizes.isEmpty()) {
            return;
        }
        
        // Fix: Set thread context classloader so Iceberg's DynMethods can find classes
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            AppendFiles append = table.newAppend();
            
            for (java.util.Map.Entry<String, Long> entry : s3PathsWithSizes.entrySet()) {
                String s3Path = entry.getKey();
                long fileSize = entry.getValue();
                DataFile dataFile = createDataFileWithSize(s3Path, fileSize, table);
                append.appendFile(dataFile);
                logger.debug("[Iceberg] Added file to commit: {} (size: {} bytes)", s3Path, fileSize);
            }
            
            append.commit();
            
            logger.info("[Iceberg] Committed {} files for index '{}', snapshot={}", 
                       s3PathsWithSizes.size(), indexName, table.currentSnapshot().snapshotId());
            
        } catch (Exception e) {
            logger.error("[Iceberg] Commit failed for index '{}': {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Iceberg commit failed", e);
        } finally {
            // Restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    private DataFile createDataFileWithSize(String s3Path, long fileSize, Table table) throws Exception {
        // Create DataFile with actual file size from S3 upload
        // TODO: Extract actual record count from Parquet footer for better statistics
        return DataFiles.builder(table.spec())
            .withPath(s3Path)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(fileSize)  // Actual file size from upload
            .withRecordCount(1L)  // Placeholder - should extract from Parquet footer
            .build();
    }
}
