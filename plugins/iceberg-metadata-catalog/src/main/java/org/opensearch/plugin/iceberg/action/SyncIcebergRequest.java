/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to sync an index to Iceberg catalog.
 */
public class SyncIcebergRequest extends BroadcastRequest<SyncIcebergRequest> {
    
    public SyncIcebergRequest(StreamInput in) throws IOException {
        super(in);
    }
    
    public SyncIcebergRequest(String... indices) {
        super(indices);
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
