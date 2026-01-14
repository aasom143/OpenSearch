/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IndexingExecutionEngine<T extends DataFormat> extends Closeable {

    List<String> supportedFieldTypes();

    Writer<? extends DocumentInput<?>> createWriter(long writerGeneration)
        throws IOException; // A writer responsible for data format vended by this engine.

    Merger getMerger(); // Merger responsible for merging for specific data format

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();

    void loadWriterFiles(CatalogSnapshot catalogSnapshot)
        throws IOException; // Bootstrap hook to make engine aware of previously written files from CatalogSnapshot

    default long getNativeBytesUsed() {
        return 0;
    }

    /**
     * Callback invoked after files are successfully uploaded to remote store.
     * Engines can override this to perform post-upload actions (e.g., updating metadata catalogs).
     * 
     * @param indexName the index name
     * @param s3PathsWithSizes map of S3 paths to file sizes
     */
    default void onFilesUploadedToRemoteStore(String indexName, Map<String, Long> s3PathsWithSizes) {
        // Default: no-op
    }

    /**
     * Callback invoked after files are successfully deleted from remote store.
     * Engines can override this to perform cleanup (e.g., removing from metadata catalogs).
     * 
     * @param indexName the index name
     * @param s3Paths collection of S3 paths that were deleted
     */
    default void onFilesDeletedFromRemoteStore(String indexName, Collection<String> s3Paths) {
        // Default: no-op
    }

    /**
     * Reconcile engine's catalog/metadata with current active files in remote store.
     * Called after successful metadata upload to ensure catalogs stay in sync.
     * Engines can override this to remove stale entries from their metadata systems.
     * 
     * @param indexName the index name
     * @param shardId the shard ID for shard-aware reconciliation
     * @param activeS3Paths collection of currently active S3 paths for this shard
     */
    default void reconcileWithActiveFiles(String indexName, int shardId, Collection<String> activeS3Paths) {
        // Default: no-op
    }

    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;
}
