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
    
    private String roleArn;
    private String s3Bucket;
    private String region;
    
    public SyncIcebergRequest(StreamInput in) throws IOException {
        super(in);
        this.roleArn = in.readOptionalString();
        this.s3Bucket = in.readOptionalString();
        this.region = in.readOptionalString();
    }
    
    public SyncIcebergRequest(String indexName) {
        super(new String[]{indexName});
    }
    
    public SyncIcebergRequest(String indexName, String roleArn, String s3Bucket, String region) {
        super(new String[]{indexName});
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
}
