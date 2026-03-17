/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DatafusionSearcher implements EngineSearcher<DatafusionQuery, RecordBatchStream> {
    private static final Logger logger = LogManager.getLogger(DatafusionSearcher.class);
    private final String source;
    private DatafusionReader reader;
    private Closeable closeable;

    public DatafusionSearcher(String source, DatafusionReader reader, Closeable close) {
        this.source = source;
        this.reader = reader;
        this.closeable = close;
    }

    @Override
    public String source() {
        return source;
    }


    @Override
    public long search(DatafusionQuery datafusionQuery, Long runtimePtr) {
        if (datafusionQuery.isFetchPhase()) {
            long[] row_ids = datafusionQuery.getQueryPhaseRowIds()
                .stream()
                .mapToLong(Long::longValue)
                .toArray();
            String[] includeFields = Objects.isNull(datafusionQuery.getIncludeFields()) ? new String[]{} : datafusionQuery.getIncludeFields().toArray(String[]::new);
            String[] excludeFields = Objects.isNull(datafusionQuery.getExcludeFields()) ? new String[]{} : datafusionQuery.getExcludeFields().toArray(String[]::new);

            return NativeBridge.executeFetchPhase(reader.getReaderPtr(), row_ids, includeFields, excludeFields, runtimePtr);
        }
        throw new RuntimeException("Can be only called for fetch phase");
    }

    @Override
    public CompletableFuture<Long> searchAsync(DatafusionQuery datafusionQuery, Long runtimePtr) {
        
        // Check if we should use downloaded partition approach
        if (datafusionQuery.useDownloadedPartition()) {
            
            return searchAsyncWithDownloadedPartition(
                datafusionQuery.getLocalDownloadDir(),
                datafusionQuery.getTableBucketArn(),
                datafusionQuery.getDatabaseName(),
                datafusionQuery.getIcebergTableName(),
                datafusionQuery.getPartitionColumn(),
                datafusionQuery.getPartitionValue(),
                datafusionQuery.getS3Options(),
                datafusionQuery.getSubstraitBytes(),
                datafusionQuery.getQueryPlanExplainEnabled(),
                runtimePtr
            );
        }
        
        // Default: use direct S3 Tables access
        CompletableFuture<Long> result = new CompletableFuture<>();
        NativeBridge.executeQueryPhaseAsync(reader.getReaderPtr(), datafusionQuery.getIndexName(), datafusionQuery.getSubstraitBytes(), datafusionQuery.getQueryPlanExplainEnabled(), runtimePtr, new ActionListener<Long>() {
            @Override
            public void onResponse(Long streamPointer) {
                if (streamPointer == 0) {
                    result.complete(0L);
                } else {
                    result.complete(streamPointer);
                }
            }

            @Override
            public void onFailure(Exception e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /**
     * Execute query by downloading S3 table partition files to local directory first.
     * This approach is useful when you want to cache partition data locally for repeated queries.
     *
     * @param localDir Local directory path where files will be downloaded
     * @param tableBucketArn S3 Tables bucket ARN
     * @param databaseName Database/namespace name
     * @param tableName Table name
     * @param partitionColumn Partition column name (e.g., "shard_id")
     * @param partitionValue Partition value to download (e.g., "0")
     * @param s3Options Map containing S3 credentials (access key, secret key, session token, region)
     * @param substraitBytes Serialized Substrait query plan
     * @param isQueryPlanExplainEnabled Enable query plan explanation
     * @param runtimePtr Pointer to DataFusion runtime
     * @return CompletableFuture with stream pointer
     */
    public CompletableFuture<Long> searchAsyncWithDownloadedPartition(
        String localDir,
        String tableBucketArn,
        String databaseName,
        String tableName,
        String partitionColumn,
        String partitionValue,
        java.util.Map<String, String> s3Options,
        byte[] substraitBytes,
        boolean isQueryPlanExplainEnabled,
        Long runtimePtr
    ) {
        if (s3Options != null) {
            for (java.util.Map.Entry<String, String> entry : s3Options.entrySet()) {
            }
        }

        CompletableFuture<Long> result = new CompletableFuture<>();

        NativeBridge.executeQueryWithDownloadedPartitionAsync(
            localDir,
            tableBucketArn,
            databaseName,
            tableName,
            partitionColumn,
            partitionValue,
            s3Options,
            substraitBytes,
            isQueryPlanExplainEnabled,
            runtimePtr,
            new ActionListener<Long>() {
                @Override
                public void onResponse(Long streamPointer) {
                    result.complete(streamPointer != null ? streamPointer : 0L);
                }

                @Override
                public void onFailure(Exception e) {
                    result.completeExceptionally(e);
                }
            }
        );

        return result;
    }


    public DatafusionReader getReader() {
        return reader;
    }

    @Override
    public void close() {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close", e);
        } catch (AlreadyClosedException e) {
            // This means there's a bug somewhere: don't suppress it
            throw new AssertionError(e);
        }

    }
}
