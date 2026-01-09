/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.Nullable;
import org.opensearch.index.shard.RemoteUploadCallback;

import java.io.IOException;

public interface Writer<P extends DocumentInput<?>> {

    WriteResult addDoc(P d) throws IOException;

    FileInfos flush(FlushIn flushIn) throws IOException;

    void sync() throws IOException;

    void close();

    P newDocumentInput();

    /**
     * Get callback to be invoked after successful remote upload.
     * Allows format-specific writers to perform post-upload actions (e.g., metadata updates).
     * 
     * @return callback, or null if writer doesn't need remote upload notifications
     */
    @Nullable
    default RemoteUploadCallback getRemoteUploadCallback() {
        return null;
    }
}
