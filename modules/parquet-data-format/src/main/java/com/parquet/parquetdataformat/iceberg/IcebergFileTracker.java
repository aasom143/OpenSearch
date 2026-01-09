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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Tracks Parquet files written during flush operations and commits them
 * to Iceberg during refresh operations.
 */
public class IcebergFileTracker {
    private static final Logger logger = LogManager.getLogger(IcebergFileTracker.class);
    private final List<String> pendingFiles = new ArrayList<>();
    
    public IcebergFileTracker() {
        // No longer needs indexName parameter - extracts from file paths
    }
    
    /**
     * Track a file that was flushed but not yet committed to Iceberg.
     */
    public synchronized void trackFile(String filePath) {
        pendingFiles.add(filePath);
    }
    
    /**
     * Commit all pending files to Iceberg (called during refresh).
     * @deprecated Use commitFiles(List) instead to commit specific files after remote upload.
     */
    @Deprecated
    public synchronized void commitPending() {
        if (pendingFiles.isEmpty()) {
            return;
        }
        commitFiles(new ArrayList<>(pendingFiles));
        pendingFiles.clear();
    }

    /**
     * Commit specific files to Iceberg after successful remote upload.
     * This method should be called with S3 paths, not local paths.
     * 
     * @param filePaths List of file paths (S3 paths) that were successfully uploaded to remote store
     */
    public synchronized void commitFiles(List<String> filePaths) {
        if (filePaths.isEmpty()) {
            return;
        }
        
        // Extract index name from first file path
        String indexName = extractIndexNameFromPath(filePaths.get(0));
        commitFilesForIndex(indexName, filePaths);
    }
    
    /**
     * Commit specific S3 files to Iceberg for a given index.
     * 
     * @param indexName The index name to use for Iceberg table identification
     * @param s3Paths List of S3 paths that were successfully uploaded to remote store
     */
    public synchronized void commitFilesForIndex(String indexName, List<String> s3Paths) {
        if (s3Paths.isEmpty()) {
            return;
        }
        
        // Fix: Set thread context classloader so Iceberg's DynMethods can find classes
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName);
            AppendFiles append = table.newAppend();
            
            for (String s3Path : s3Paths) {
                DataFile dataFile = createDataFile(s3Path, table);
                append.appendFile(dataFile);
                logger.debug("[Iceberg] Added file to commit: {}", s3Path);
            }
            
            append.commit();
            
            logger.info("[Iceberg] Committed {} files for index '{}', snapshot={}", 
                       s3Paths.size(), indexName, table.currentSnapshot().snapshotId());
            
        } catch (Exception e) {
            logger.error("[Iceberg] Commit failed for index '{}': {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Iceberg commit failed", e);
        } finally {
            // Restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Clear all pending files without committing.
     * Used when files need to be re-tracked after remote upload.
     */
    public synchronized void clearPending() {
        pendingFiles.clear();
    }
    
    private DataFile createDataFile(String s3Path, Table table) throws Exception {
        // For S3 paths, we cannot use Files.size() as it expects local files
        // Using 0 as placeholder - Iceberg requires size >= 0
        // TODO: Pass actual file size from RemoteStoreRefreshListener where it's already available
        
        return DataFiles.builder(table.spec())
            .withPath(s3Path)
            .withFormat(FileFormat.PARQUET)  // Required: file format
            .withFileSizeInBytes(0L)  // Required: size >= 0 (using placeholder, should pass actual size)
            .withRecordCount(0L)  // Required: count >= 0 (TODO: extract from Parquet footer)
            .build();
    }
    
    /**
     * Extract index name from file path.
     */
    private String extractIndexNameFromPath(String filePath) {
        try {
            Path path = Path.of(filePath).toAbsolutePath();
            // Look for common OpenSearch path patterns
            for (int i = 0; i < path.getNameCount() - 1; i++) {
                String segment = path.getName(i).toString();
                if ("indices".equals(segment)) {
                    return path.getName(i + 1).toString();
                }
            }
            // Fallback: use parent directory name
            return path.getParent() != null ? path.getParent().getFileName().toString() : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
}
