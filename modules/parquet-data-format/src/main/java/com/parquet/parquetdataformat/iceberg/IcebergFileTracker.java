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
    
    /**
     * Remove files from Iceberg catalog (cleanup after S3 deletion).
     * Uses Iceberg DELETE transaction to remove file references.
     * 
     * @param indexName The index name
     * @param s3Paths Collection of S3 paths to remove
     */
    public synchronized void removeFiles(String indexName, java.util.Collection<String> s3Paths) {
        if (s3Paths.isEmpty()) {
            return;
        }
        
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            org.apache.iceberg.DeleteFiles delete = table.newDelete();
            
            for (String s3Path : s3Paths) {
                delete.deleteFile(s3Path);
                logger.debug("[Iceberg] Marked for deletion: {}", s3Path);
            }
            
            delete.commit();
            
            logger.info("[Iceberg] Removed {} files from catalog for index '{}', snapshot={}", 
                       s3Paths.size(), indexName, table.currentSnapshot().snapshotId());
            
        } catch (Exception e) {
            logger.error("[Iceberg] Failed to remove files for index '{}': {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Iceberg file removal failed", e);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    /**
     * Reconcile Iceberg catalog with current active files for a specific shard.
     * Only removes files from THIS shard's path, leaving other shards' files untouched.
     * This ensures the catalog stays in sync with actual segment state after merges and cleanup.
     * 
     * @param indexName The index name
     * @param shardId The shard ID for shard-aware reconciliation
     * @param activeS3Paths Collection of currently active S3 paths for this shard
     */
    public synchronized void reconcileCatalog(String indexName, int shardId, java.util.Collection<String> activeS3Paths) {
        if (activeS3Paths == null || activeS3Paths.isEmpty()) {
            logger.warn("[Iceberg] Skipping reconciliation - no active files provided for index '{}'", indexName);
            return;
        }
        
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            
            if (table.currentSnapshot() == null) {
                logger.debug("[Iceberg] No snapshot exists yet for index '{}', skipping reconciliation", indexName);
                return;
            }
            
            // Get ALL current files in Iceberg catalog using table scan
            // This automatically handles manifest reading and filtering of deleted files
            java.util.Set<String> catalogFiles = new java.util.HashSet<>();
            try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks = 
                    table.newScan().planFiles()) {
                for (org.apache.iceberg.FileScanTask task : tasks) {
                    catalogFiles.add(task.file().path().toString());
                }
            }
            
            // Build shard path pattern: /{shardId}/segments/
            String shardPathPattern = "/" + shardId + "/segments/";
            
            // Filter catalog files to only those from THIS shard
            java.util.Set<String> thisShardCatalogFiles = new java.util.HashSet<>();
            for (String catalogFile : catalogFiles) {
                if (catalogFile.contains(shardPathPattern)) {
                    thisShardCatalogFiles.add(catalogFile);
                }
            }
            
            logger.info("[Iceberg] Reconciliation for index '{}' shard {}: totalCatalogFiles={}, thisShardCatalogFiles={}, activeFiles={}", 
                       indexName, shardId, catalogFiles.size(), thisShardCatalogFiles.size(), activeS3Paths.size());
            logger.info("[Iceberg] This shard's catalog files: {}", thisShardCatalogFiles);
            logger.info("[Iceberg] Active files for this shard: {}", activeS3Paths);
            
            // Find stale files: in THIS shard's catalog but not in active set
            java.util.Set<String> activeSet = new java.util.HashSet<>(activeS3Paths);
            java.util.Set<String> staleFiles = new java.util.HashSet<>();
            for (String catalogFile : thisShardCatalogFiles) {
                if (!activeSet.contains(catalogFile)) {
                    staleFiles.add(catalogFile);
                }
            }
            
            logger.info("[Iceberg] Stale files to remove for shard {}: {}", shardId, staleFiles);
            
            // Remove stale files if any exist
            if (!staleFiles.isEmpty()) {
                logger.info("[Iceberg] Reconciling catalog for index '{}': removing {} stale files", 
                           indexName, staleFiles.size());
                
                org.apache.iceberg.DeleteFiles delete = table.newDelete();
                for (String staleFile : staleFiles) {
                    delete.deleteFile(staleFile);
                    logger.debug("[Iceberg] Removing stale file from catalog: {}", staleFile);
                }
                
                delete.commit();
                logger.info("[Iceberg] Reconciliation complete for index '{}', snapshot={}", 
                           indexName, table.currentSnapshot().snapshotId());
            } else {
                logger.debug("[Iceberg] No stale files found for index '{}', catalog is in sync", indexName);
            }
            
        } catch (Exception e) {
            logger.error("[Iceberg] Failed to reconcile catalog for index '{}': {}", indexName, e.getMessage(), e);
            // Don't throw - reconciliation failure shouldn't block refresh
        } finally {
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
