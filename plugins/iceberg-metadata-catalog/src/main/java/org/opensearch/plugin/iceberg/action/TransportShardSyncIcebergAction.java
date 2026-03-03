/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.iceberg.service.IcebergService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Per-shard transport action that syncs Parquet files to Iceberg catalog.
 * Executes on the node that holds the shard.
 */
public class TransportShardSyncIcebergAction extends TransportReplicationAction<
    ShardSyncIcebergRequest,
    ShardSyncIcebergRequest,
    ReplicationResponse> {
    
    private static final Logger logger = LogManager.getLogger(TransportShardSyncIcebergAction.class);
    public static final String NAME = "indices:admin/iceberg/sync/shard";
    
    private final IcebergService icebergService;
    
    @Inject
    public TransportShardSyncIcebergAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IcebergService icebergService
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardSyncIcebergRequest::new,
            ShardSyncIcebergRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.icebergService = icebergService;
    }
    
    @Override
    protected ReplicationResponse newResponseInstance(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }
    
    @Override
    protected void shardOperationOnPrimary(
        ShardSyncIcebergRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardSyncIcebergRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            ShardId shardId = request.shardId();
            logger.info("[Iceberg Shard Sync] Syncing shard: {} (role={}, bucket={}, region={})", 
                       shardId, request.getRoleArn(), request.getS3Bucket(), request.getRegion());
            
            // Perform sync for this shard using IcebergService with role parameters
            Map<String, Object> result = icebergService.syncShard(
                shardId, 
                request.getRoleArn(), 
                request.getS3Bucket(), 
                request.getRegion()
            );
            
            logger.info("[Iceberg Shard Sync] Shard {} synced: {}", shardId, result);
            
            ReplicationResponse response = new ReplicationResponse();
            return new PrimaryResult<>(request, response);
        });
    }
    
    @Override
    protected void shardOperationOnReplica(ShardSyncIcebergRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        // No-op on replicas - only primary needs to sync
        ActionListener.completeWith(listener, () -> new ReplicaResult());
    }
}
