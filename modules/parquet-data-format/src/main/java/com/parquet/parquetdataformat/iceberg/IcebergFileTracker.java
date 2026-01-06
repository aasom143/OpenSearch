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
     */
    public synchronized void commitPending() {
        if (pendingFiles.isEmpty()) {
            return;
        }
        
        // Extract index name from first file path
        String indexName = extractIndexNameFromPath(pendingFiles.get(0));
        
        // Fix: Set thread context classloader so Iceberg's DynMethods can find classes
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName);
            AppendFiles append = table.newAppend();
            
            for (String filePath : pendingFiles) {
                DataFile dataFile = createDataFile(filePath, table);
                append.appendFile(dataFile);
            }
            
            append.commit();
            
            logger.info("[Iceberg] Refresh committed {} files for index '{}', snapshot={}", 
                       pendingFiles.size(), indexName, table.currentSnapshot().snapshotId());
            
            pendingFiles.clear();
        } catch (Exception e) {
            logger.error("[Iceberg] Refresh commit failed for index '{}': {}", indexName, e.getMessage(), e);
            // Keep pendingFiles for retry on next refresh
        } finally {
            // Restore original classloader
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }
    
    private DataFile createDataFile(String filePath, Table table) throws Exception {
        Path path = Path.of(filePath);
        long fileSize = Files.size(path);
        
        return DataFiles.builder(table.spec())
            .withPath(filePath)
            .withFileSizeInBytes(fileSize)
            .withRecordCount(0) // TODO: Extract from Parquet footer
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
