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
 * to Iceberg tables in S3 Tables during refresh operations.
 * 
 * S3 Tables provides native Iceberg support with automatic metadata management.
 */
public class S3TablesIcebergFileTracker {
    private static final Logger logger = LogManager.getLogger(S3TablesIcebergFileTracker.class);
    private final org.apache.iceberg.Schema icebergSchema;
    
    public S3TablesIcebergFileTracker(org.apache.iceberg.Schema icebergSchema) {
        this.icebergSchema = icebergSchema;
        logger.info("[Iceberg S3Tables] FileTracker initialized with schema containing {} fields", 
                   icebergSchema.columns().size());
    }
    
    /**
     * Commit specific S3 files with actual sizes to Iceberg in S3 Tables for a given index.
     * 
     * @param indexName The index name to use for Iceberg table identification
     * @param s3PathsWithSizes Map of S3 paths to their actual file sizes in bytes
     */
    public synchronized void commitFilesWithSizes(String indexName, java.util.Map<String, Long> s3PathsWithSizes) {
        logger.info("[Iceberg S3Tables] commitFilesWithSizes called: index={}, fileCount={}", 
                   indexName, s3PathsWithSizes.size());
        
        if (s3PathsWithSizes.isEmpty()) {
            logger.warn("[Iceberg S3Tables] Empty s3PathsWithSizes, returning");
            return;
        }
        
        // Fix: Set thread context classloader so Iceberg's DynMethods can find classes
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        logger.info("[Iceberg S3Tables] Set context classloader, calling S3TablesIcebergManager.getInstance()");
        
        try {
            Table table = S3TablesIcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            logger.info("[Iceberg S3Tables] Got table from manager: {}", (table != null ? "SUCCESS" : "NULL"));
            if (table == null) {
                logger.error("[Iceberg S3Tables] Failed to get or create table for index '{}'", indexName);
                return;
            }
            
            // Copy files to S3 Tables warehouse location and get new paths
            java.util.Map<String, Long> warehousePathsWithSizes = copyFilesToWarehouse(s3PathsWithSizes, table);
            
            AppendFiles append = table.newAppend();
            
            for (java.util.Map.Entry<String, Long> entry : warehousePathsWithSizes.entrySet()) {
                String warehousePath = entry.getKey();
                long fileSize = entry.getValue();
                DataFile dataFile = createDataFileWithSize(warehousePath, fileSize, table);
                append.appendFile(dataFile);
                logger.debug("[Iceberg S3Tables] Added file to commit: {} (size: {} bytes)", warehousePath, fileSize);
            }
            
            append.commit();
            
            logger.info("[Iceberg S3Tables] Committed {} files for index '{}' to S3 Tables, snapshot={}", 
                       warehousePathsWithSizes.size(), indexName, table.currentSnapshot().snapshotId());
            
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Commit failed for index '{}': {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Iceberg S3 Tables commit failed", e);
        } finally {
            // Restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    /**
     * Copy files from original S3 location to S3 Tables warehouse location using Iceberg's S3FileIO.
     * Preserves the original directory structure to maintain shard-aware reconciliation.
     * 
     * @param originalPathsWithSizes Map of original S3 paths to file sizes
     * @param table Iceberg table (to get warehouse location and FileIO)
     * @return Map of new warehouse paths to file sizes
     */
    private java.util.Map<String, Long> copyFilesToWarehouse(
            java.util.Map<String, Long> originalPathsWithSizes, 
            Table table) throws Exception {
        
        java.util.Map<String, Long> warehousePathsWithSizes = new java.util.HashMap<>();
        String tableLocation = table.location();
        
        logger.info("[Iceberg S3Tables] Table location: {}, copying {} files to warehouse", 
                   tableLocation, originalPathsWithSizes.size());
        
        // Use Iceberg's FileIO for S3 operations (avoids jar hell with AWS SDK)
        org.apache.iceberg.io.FileIO fileIO = table.io();
        
        for (java.util.Map.Entry<String, Long> entry : originalPathsWithSizes.entrySet()) {
            String originalPath = entry.getKey();
            long fileSize = entry.getValue();
            
            // Extract path structure after "repository/" to preserve shard info
            // Original: s3://bucket/repository/{uuid}/{shardId}/segments/data/parquet/file.parquet
            // Extract: {uuid}/{shardId}/segments/data/parquet/file.parquet
            int repoIndex = originalPath.indexOf("repository/");
            String pathAfterRepo = originalPath.substring(repoIndex + "repository/".length());
            
            // Construct warehouse path preserving original structure
            // Result: s3://srirasac-test/warehouse/{uuid}/{shardId}/segments/data/parquet/file.parquet
            String warehousePath = tableLocation + "/" + pathAfterRepo;
            
            logger.info("[Iceberg S3Tables] Copying: {} -> {}", originalPath, warehousePath);
            
            try {
                // Read from original location using FileIO
                org.apache.iceberg.io.InputFile inputFile = fileIO.newInputFile(originalPath);
                
                // Write to warehouse location using FileIO
                org.apache.iceberg.io.OutputFile outputFile = fileIO.newOutputFile(warehousePath);
                
                // Copy data
                try (java.io.InputStream in = inputFile.newStream();
                     java.io.OutputStream out = outputFile.createOrOverwrite()) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
                
                warehousePathsWithSizes.put(warehousePath, fileSize);
                logger.debug("[Iceberg S3Tables] Successfully copied file to warehouse: {}", warehousePath);
                
            } catch (Exception e) {
                logger.error("[Iceberg S3Tables] Failed to copy file {} to warehouse: {}", 
                           originalPath, e.getMessage(), e);
                throw e;
            }
        }
        
        logger.info("[Iceberg S3Tables] Copied {} files to warehouse location", warehousePathsWithSizes.size());
        return warehousePathsWithSizes;
    }
    
    /**
     * Remove files from Iceberg catalog in S3 Tables (cleanup after S3 deletion).
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
            Table table = S3TablesIcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            if (table == null) {
                logger.warn("[Iceberg S3Tables] Table not found for index '{}', skipping file removal", indexName);
                return;
            }
            
            org.apache.iceberg.DeleteFiles delete = table.newDelete();
            
            for (String s3Path : s3Paths) {
                delete.deleteFile(s3Path);
                logger.debug("[Iceberg S3Tables] Marked for deletion: {}", s3Path);
            }
            
            delete.commit();
            
            logger.info("[Iceberg S3Tables] Removed {} files from S3 Tables catalog for index '{}', snapshot={}", 
                       s3Paths.size(), indexName, table.currentSnapshot().snapshotId());
            
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Failed to remove files for index '{}': {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Iceberg S3 Tables file removal failed", e);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    /**
     * Reconcile Iceberg catalog with current active files for a specific shard.
     * Only removes files from THIS shard's path, leaving other shards' files untouched.
     * This ensures the S3 Tables catalog stays in sync with actual segment state after merges and cleanup.
     * 
     * @param indexName The index name
     * @param shardId The shard ID for shard-aware reconciliation
     * @param activeS3Paths Collection of currently active S3 paths for this shard
     */
    public synchronized void reconcileCatalog(String indexName, int shardId, java.util.Collection<String> activeS3Paths) {
        if (activeS3Paths == null || activeS3Paths.isEmpty()) {
            logger.warn("[Iceberg S3Tables] Skipping reconciliation - no active files provided for index '{}'", indexName);
            return;
        }
        
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = S3TablesIcebergManager.getInstance().getOrCreateTable(indexName, icebergSchema);
            if (table == null) {
                logger.warn("[Iceberg S3Tables] Table not found for index '{}', skipping reconciliation", indexName);
                return;
            }
            
            if (table.currentSnapshot() == null) {
                logger.debug("[Iceberg S3Tables] No snapshot exists yet for index '{}', skipping reconciliation", indexName);
                return;
            }
            
            // Transform activeS3Paths from original repository paths to warehouse paths
            // Original: s3://srirasac-iceberg-test/repository/{uuid}/{shardId}/segments/...
            // Warehouse: s3://srirasac-test/warehouse/{uuid}/{shardId}/segments/...
            String tableLocation = table.location();
            java.util.Set<String> activeWarehousePaths = new java.util.HashSet<>();
            for (String originalPath : activeS3Paths) {
                int repoIndex = originalPath.indexOf("repository/");
                String pathAfterRepo = originalPath.substring(repoIndex + "repository/".length());
                String warehousePath = tableLocation + "/" + pathAfterRepo;
                activeWarehousePaths.add(warehousePath);
            }
            
            logger.info("[Iceberg S3Tables] Transformed {} original paths to warehouse paths", activeWarehousePaths.size());
            logger.debug("[Iceberg S3Tables] Sample warehouse path: {}", 
                       activeWarehousePaths.isEmpty() ? "none" : activeWarehousePaths.iterator().next());
            
            // Get ALL current files in S3 Tables Iceberg catalog using table scan
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
            
            logger.info("[Iceberg S3Tables] Reconciliation for index '{}' shard {}: totalCatalogFiles={}, thisShardCatalogFiles={}, activeWarehousePaths={}", 
                       indexName, shardId, catalogFiles.size(), thisShardCatalogFiles.size(), activeWarehousePaths.size());
            logger.debug("[Iceberg S3Tables] This shard's catalog files: {}", thisShardCatalogFiles);
            logger.debug("[Iceberg S3Tables] Active warehouse paths for this shard: {}", activeWarehousePaths);
            
            // Find stale files: in THIS shard's catalog but not in active warehouse paths
            java.util.Set<String> staleFiles = new java.util.HashSet<>();
            for (String catalogFile : thisShardCatalogFiles) {
                if (!activeWarehousePaths.contains(catalogFile)) {
                    staleFiles.add(catalogFile);
                }
            }
            
            logger.debug("[Iceberg S3Tables] Stale files to remove for shard {}: {}", shardId, staleFiles);
            
            // Remove stale files if any exist
            if (!staleFiles.isEmpty()) {
                logger.info("[Iceberg S3Tables] Reconciling catalog for index '{}': removing {} stale files from S3 Tables", 
                           indexName, staleFiles.size());
                
                org.apache.iceberg.DeleteFiles delete = table.newDelete();
                for (String staleFile : staleFiles) {
                    delete.deleteFile(staleFile);
                    logger.debug("[Iceberg S3Tables] Removing stale file from S3 Tables catalog: {}", staleFile);
                }
                
                delete.commit();
                logger.info("[Iceberg S3Tables] Reconciliation complete for index '{}', snapshot={}", 
                           indexName, table.currentSnapshot().snapshotId());
            } else {
                logger.debug("[Iceberg S3Tables] No stale files found for index '{}', S3 Tables catalog is in sync", indexName);
            }
            
        } catch (Exception e) {
            logger.error("[Iceberg S3Tables] Failed to reconcile catalog for index '{}': {}", indexName, e.getMessage(), e);
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
