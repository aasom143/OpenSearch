/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;
import java.util.Map;

/**
 * Callback interface for writers to be notified after successful remote upload.
 * Allows format-specific plugins to perform post-upload actions (e.g., metadata updates).
 * 
 * This interface enables plugins to maintain external metadata systems (like Iceberg catalogs)
 * in sync with files uploaded to remote storage, without coupling server code to specific
 * metadata implementations.
 *
 * @opensearch.internal
 */
public interface RemoteUploadCallback {
    /**
     * Called after a batch of files has been successfully uploaded to remote store.
     * 
     * @param uploadedFiles the files that were successfully uploaded to remote storage
     * @param fileSizes map of local filename to file size in bytes
     */
    void onRemoteUploadSuccess(Collection<FileMetadata> uploadedFiles, Map<String, Long> fileSizes);
}
