/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Per-shard request to sync Parquet files to Iceberg.
 */
public class ShardSyncIcebergRequest extends ReplicationRequest<ShardSyncIcebergRequest> {
    
    private String roleArn;
    private String s3Bucket;
    private String region;
    
    public ShardSyncIcebergRequest(StreamInput in) throws IOException {
        super(in);
        this.roleArn = in.readOptionalString();
        this.s3Bucket = in.readOptionalString();
        this.region = in.readOptionalString();
    }
    
    public ShardSyncIcebergRequest(ShardId shardId) {
        super(shardId);
    }
    
    public ShardSyncIcebergRequest(ShardId shardId, String roleArn, String s3Bucket, String region) {
        super(shardId);
        this.roleArn = roleArn;
        this.s3Bucket = s3Bucket;
        this.region = region;
    }
    
    public String getRoleArn() {
        return roleArn;
    }
    
    public String getS3Bucket() {
        return s3Bucket;
    }
    
    public String getRegion() {
        return region;
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(roleArn);
        out.writeOptionalString(s3Bucket);
        out.writeOptionalString(region);
    }
    
    @Override
    public String toString() {
        return "ShardSyncIcebergRequest{" +
            "shardId=" + shardId() +
            ", roleArn=" + roleArn +
            ", s3Bucket=" + s3Bucket +
            ", region=" + region +
            ", timeout=" + timeout() +
            '}';
    }
}
