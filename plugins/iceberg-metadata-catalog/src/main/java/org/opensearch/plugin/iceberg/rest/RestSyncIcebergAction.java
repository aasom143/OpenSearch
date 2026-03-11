/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.rest;

import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.iceberg.action.SyncIcebergAction;
import org.opensearch.plugin.iceberg.action.SyncIcebergRequest;
import org.opensearch.plugin.iceberg.action.SyncIcebergResponse;
import org.opensearch.plugin.iceberg.service.IcebergService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for syncing Parquet files to Iceberg catalog across all nodes.
 * 
 * Endpoint: POST /_iceberg/sync?index={indexName}
 * 
 * Reads role_arn, bucket, region from cluster settings:
 *   datafusion.iceberg.s3tables.role_arn
 *   datafusion.iceberg.s3tables.bucket
 *   datafusion.iceberg.s3tables.region
 */
public class RestSyncIcebergAction extends BaseRestHandler {

    private final ClusterService clusterService;

    public RestSyncIcebergAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return "iceberg_sync_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_iceberg/sync")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexName = request.param("index");
        
        if (indexName == null || indexName.isEmpty()) {
            return channel -> {
                XContentBuilder builder = channel.newErrorBuilder();
                builder.startObject();
                builder.field("error", "index parameter is required");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            };
        }

        String roleArn = clusterService.getClusterSettings().get(IcebergService.S3TABLES_ROLE_ARN_SETTING);
        String s3Bucket = clusterService.getClusterSettings().get(IcebergService.S3TABLES_BUCKET_SETTING);
        String region = clusterService.getClusterSettings().get(IcebergService.S3TABLES_REGION_SETTING);

        StringBuilder missing = new StringBuilder();
        if (roleArn == null || roleArn.isEmpty()) missing.append("datafusion.iceberg.s3tables.role_arn ");
        if (s3Bucket == null || s3Bucket.isEmpty()) missing.append("datafusion.iceberg.s3tables.bucket ");
        if (region == null || region.isEmpty()) missing.append("datafusion.iceberg.s3tables.region ");

        if (missing.length() > 0) {
            return channel -> {
                XContentBuilder builder = channel.newErrorBuilder();
                builder.startObject();
                builder.field("error", "Required cluster settings not configured: " + missing.toString().trim());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            };
        }

        SyncIcebergRequest syncRequest = new SyncIcebergRequest(indexName, roleArn, s3Bucket, region);
        
        return channel -> client.execute(
            SyncIcebergAction.INSTANCE,
            syncRequest,
            new ActionListener<SyncIcebergResponse>() {
                @Override
                public void onResponse(SyncIcebergResponse response) {
                    try {
                        XContentBuilder builder = channel.newBuilder();
                        builder.startObject();
                        builder.field("index", indexName);
                        builder.field("success", true);
                        builder.field("total_shards", response.getTotalShards());
                        builder.field("successful_shards", response.getSuccessfulShards());
                        builder.field("failed_shards", response.getFailedShards());
                        builder.field("files_added", response.getFilesAdded());
                        builder.field("files_removed", response.getFilesRemoved());
                        builder.field("files_kept", response.getFilesKept());
                        builder.endObject();
                        
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
                
                @Override
                public void onFailure(Exception e) {
                    try {
                        XContentBuilder builder = channel.newErrorBuilder();
                        builder.startObject();
                        builder.field("error", e.getMessage());
                        builder.field("index", indexName);
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
                    } catch (IOException ex) {
                        channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, ex.getMessage()));
                    }
                }
            }
        );
    }
}
