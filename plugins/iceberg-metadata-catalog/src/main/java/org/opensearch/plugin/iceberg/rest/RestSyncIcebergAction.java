/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.rest;

import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.iceberg.action.SyncIcebergAction;
import org.opensearch.plugin.iceberg.action.SyncIcebergRequest;
import org.opensearch.plugin.iceberg.action.SyncIcebergResponse;
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
 * Broadcasts to all shards of the index across all nodes.
 */
public class RestSyncIcebergAction extends BaseRestHandler {

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

        SyncIcebergRequest syncRequest = new SyncIcebergRequest(indexName);
        
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
