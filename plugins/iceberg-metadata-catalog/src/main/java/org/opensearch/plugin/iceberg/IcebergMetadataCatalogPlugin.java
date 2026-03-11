/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.iceberg;

import org.opensearch.transport.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.iceberg.rest.RestSyncIcebergAction;
import org.opensearch.plugin.iceberg.service.IcebergService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin for managing Iceberg table metadata for OpenSearch indices.
 * Provides REST API to sync Parquet files from remote store to Iceberg catalogs (Glue or S3 Tables).
 */
public class IcebergMetadataCatalogPlugin extends Plugin implements ActionPlugin {

    private IcebergService icebergService;
    private ClusterService clusterService;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.icebergService = new IcebergService(
            clusterService,
            repositoriesServiceSupplier,
            threadPool,
            environment.settings()
        );
        this.clusterService = clusterService;
        
        return Collections.singletonList(icebergService);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Collections.singletonList(
            new RestSyncIcebergAction(clusterService)
        );
    }
    
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        return Collections.singletonList(
            new ActionHandler<>(
                org.opensearch.plugin.iceberg.action.SyncIcebergAction.INSTANCE,
                org.opensearch.plugin.iceberg.action.TransportSyncIcebergAction.class
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            IcebergService.CATALOG_TYPE_SETTING,
            IcebergService.S3_TABLES_BUCKET_ARN_SETTING,
            IcebergService.AWS_REGION_SETTING,
            IcebergService.CREDENTIALS_FILE_PATH_SETTING,
            IcebergService.S3TABLES_ROLE_ARN_SETTING,
            IcebergService.S3TABLES_BUCKET_SETTING,
            IcebergService.S3TABLES_REGION_SETTING,
            IcebergService.S3TABLES_NAMESPACE_SETTING
        );
    }
}
