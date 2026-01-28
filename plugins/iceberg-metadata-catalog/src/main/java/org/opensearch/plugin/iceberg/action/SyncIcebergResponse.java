/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response from syncing an index to Iceberg catalog.
 */
public class SyncIcebergResponse extends BroadcastResponse {
    
    private int filesAdded;
    private int filesRemoved;
    private int filesKept;
    
    public SyncIcebergResponse(StreamInput in) throws IOException {
        super(in);
        filesAdded = in.readVInt();
        filesRemoved = in.readVInt();
        filesKept = in.readVInt();
    }
    
    public SyncIcebergResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        int filesAdded,
        int filesRemoved,
        int filesKept
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
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
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.toXContent(builder, params);
        builder.field("files_added", filesAdded);
        builder.field("files_removed", filesRemoved);
        builder.field("files_kept", filesKept);
        builder.endObject();
        return builder;
    }
}
