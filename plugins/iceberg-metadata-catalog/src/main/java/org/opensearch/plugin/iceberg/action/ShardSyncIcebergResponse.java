/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Per-shard response from syncing to Iceberg.
 */
public class ShardSyncIcebergResponse extends ReplicationResponse {
    
    private int filesAdded;
    private int filesRemoved;
    private int filesKept;
    
    public ShardSyncIcebergResponse(StreamInput in) throws IOException {
        super(in);
        filesAdded = in.readVInt();
        filesRemoved = in.readVInt();
        filesKept = in.readVInt();
    }
    
    public ShardSyncIcebergResponse() {
        this(0, 0, 0);
    }
    
    public ShardSyncIcebergResponse(int filesAdded, int filesRemoved, int filesKept) {
        this.filesAdded = filesAdded;
        this.filesRemoved = filesRemoved;
        this.filesKept = filesKept;
    }
    
    public int getFilesAdded() {
        return filesAdded;
    }
    
    public int getFilesRemoved() {
        return filesRemoved;
    }
    
    public int getFilesKept() {
        return filesKept;
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(filesAdded);
        out.writeVInt(filesRemoved);
        out.writeVInt(filesKept);
    }
}
