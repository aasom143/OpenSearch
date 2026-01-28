/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportBroadcastReplicationAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportService;

import java.util.List;

/**
 * Transport action to sync an index to Iceberg catalog.
 * Broadcasts to all shards of the index across all nodes.
 */
public class TransportSyncIcebergAction extends TransportBroadcastReplicationAction<
    SyncIcebergRequest,
    SyncIcebergResponse,
    ShardSyncIcebergRequest,
    ReplicationResponse> {
    
    @Inject
    public TransportSyncIcebergAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransportShardSyncIcebergAction shardSyncAction
    ) {
        super(
            SyncIcebergAction.NAME,
            SyncIcebergRequest::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            shardSyncAction
        );
    }
    
    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }
    
    @Override
    protected ShardSyncIcebergRequest newShardRequest(SyncIcebergRequest request, ShardId shardId) {
        ShardSyncIcebergRequest shardRequest = new ShardSyncIcebergRequest(shardId);
        shardRequest.waitForActiveShards(ActiveShardCount.NONE);
        return shardRequest;
    }
    
    @Override
    protected SyncIcebergResponse newResponse(
        int successfulShards,
        int failedShards,
        int totalNumCopies,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        // Aggregate results from all shard responses
        int totalAdded = 0;
        int totalRemoved = 0;
        int totalKept = 0;
        
        // Note: shard responses would need to be collected if we want accurate totals
        // For now, return basic response
        return new SyncIcebergResponse(
            totalNumCopies,
            successfulShards,
            failedShards,
            shardFailures,
            totalAdded,
            totalRemoved,
            totalKept
        );
    }
}
