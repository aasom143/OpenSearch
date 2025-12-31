/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import com.parquet.parquetdataformat.engine.ParquetExecutionEngine;
import com.parquet.parquetdataformat.iceberg.IcebergManager;
import org.apache.iceberg.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowId;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.parquet.parquetdataformat.bridge.RustBridge.mergeParquetFilesInRust;

/**
 * Implements record-batch-based merging of Parquet files.
 * Integrates with Iceberg to maintain metadata consistency.
 */
public class RecordBatchMergeStrategy implements ParquetMergeStrategy {
    
    private static final Logger logger = LogManager.getLogger(RecordBatchMergeStrategy.class);

    @Override
    public MergeResult mergeParquetFiles(List<WriterFileSet> files, long writerGeneration) {

        if (files.isEmpty()) {
            throw new IllegalArgumentException("No files to merge");
        }

        List<Path> filePaths = new ArrayList<>();
        files.forEach(writerFileSet ->  writerFileSet.getFiles().forEach(
            file -> filePaths.add(Path.of(writerFileSet.getDirectory(), file))));

        String outputDirectory = files.iterator().next().getDirectory();
        String mergedFilePath = getMergedFilePath(writerGeneration, outputDirectory);
        String mergedFileName = getMergedFileName(writerGeneration);

        // Merge files in Rust
        mergeParquetFilesInRust(filePaths, mergedFilePath);

        // Update Iceberg metadata - extract index name from path
        String indexName = extractIndexNameFromPath(outputDirectory);
        if (indexName != null) {
            updateIcebergOnMerge(files, mergedFilePath, indexName);
        }

        // Build row ID mapping
        Map<RowId, Long> rowIdMapping = new HashMap<>();

        WriterFileSet mergedWriterFileSet =
            WriterFileSet.builder().directory(Path.of(outputDirectory)).addFile(mergedFileName).writerGeneration(writerGeneration).build();


        Map<DataFormat, WriterFileSet> mergedWriterFileSetMap = Collections.singletonMap(
            new ParquetDataFormat(),
            mergedWriterFileSet
        );

        return new MergeResult(new RowIdMapping(rowIdMapping, mergedFileName), mergedWriterFileSetMap);
    }

    private String getMergedFileName(long generation) {
        // TODO
        // For debuging we have added extra "merged" in file name, later we can remove and keep same as writer
        return ParquetExecutionEngine.FILE_NAME_PREFIX + "_merged_" + generation + ParquetExecutionEngine.FILE_NAME_EXT;
    }

    private String getMergedFilePath(long generation, String outputDirectory) {
        return Path.of(outputDirectory, getMergedFileName(generation)).toString();
    }

    /**
     * Extract index name from file path.
     * Assumes path structure like: /data/indices/{indexName}/shards/{shardId}/
     */
    private String extractIndexNameFromPath(String directory) {
        try {
            Path path = Path.of(directory).toAbsolutePath();
            // Look for "indices" in the path and get the next component as index name
            for (int i = 0; i < path.getNameCount() - 1; i++) {
                if ("indices".equals(path.getName(i).toString())) {
                    return path.getName(i + 1).toString();
                }
            }
            // Fallback: use the parent directory name
            return path.getParent() != null ? path.getParent().getFileName().toString() : "unknown";
        } catch (Exception e) {
            logger.error("[Iceberg] Failed to extract index name from path: {}, error: {}", directory, e.getMessage());
            return "unknown";
        }
    }

    /**
     * Update Iceberg metadata after merge: delete old files, add merged file.
     */
    private void updateIcebergOnMerge(List<WriterFileSet> oldFiles, String mergedFilePath, String indexName) {
        try {
            Table table = IcebergManager.getInstance().getOrCreateTable(indexName);
            Transaction txn = table.newTransaction();

            // Delete old files from Iceberg
            DeleteFiles delete = txn.newDelete();
            int deletedCount = 0;
            for (WriterFileSet fs : oldFiles) {
                for (String file : fs.getFiles()) {
                    String fullPath = fs.getDirectory() + "/" + file;
                    delete.deleteFile(fullPath);
                    deletedCount++;
                }
            }
            delete.commit();

            // Add merged file to Iceberg
            AppendFiles append = txn.newAppend();
            long fileSize = Files.size(Path.of(mergedFilePath));
            DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(mergedFilePath)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(0) // TODO: Extract from Parquet footer
                .build();
            append.appendFile(dataFile);
            append.commit();

            txn.commitTransaction();

            logger.info("[Iceberg] Merge committed for index '{}': deleted {} files, added 1 merged file, snapshot={}", 
                       indexName, deletedCount, table.currentSnapshot().snapshotId());
        } catch (Exception e) {
            logger.error("[Iceberg] Merge commit failed for index '{}': {}", indexName, e.getMessage(), e);
        }
    }
}
