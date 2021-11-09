/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.plugin;

import org.opensearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import org.opensearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.GetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.search.asynchronous.management.AsynchronousSearchManagementService;
import org.opensearch.search.asynchronous.rest.RestAsynchronousSearchStatsAction;
import org.opensearch.search.asynchronous.rest.RestDeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.rest.RestGetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.rest.RestSubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.opensearch.search.asynchronous.settings.LegacyOpendistroAsynchronousSearchSettings;
import org.opensearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import org.opensearch.search.asynchronous.transport.TransportAsynchronousSearchStatsAction;
import org.opensearch.search.asynchronous.transport.TransportDeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.transport.TransportGetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.transport.TransportSubmitAsynchronousSearchAction;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.LifecycleComponent;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;


public class AsynchronousSearchPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    public static final String OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME = "opensearch_asynchronous_search_generic";
    public static final String LEGACY_OPENDISTRO_BASE_URI = "/_opendistro/_asynchronous_search";
    public static final String BASE_URI = "/_plugins/_asynchronous_search";

    private AsynchronousSearchPersistenceService persistenceService;
    private AsynchronousSearchActiveStore asynchronousSearchActiveStore;
    private AsynchronousSearchService asynchronousSearchService;

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(new SystemIndexDescriptor(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                "Stores the response for asynchronous search"));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(AsynchronousSearchManagementService.class);
    }


    //TODO Revisit these once we performance test the feature
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int availableProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        return executorBuilders;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.persistenceService = new AsynchronousSearchPersistenceService(client, clusterService, threadPool);
        this.asynchronousSearchActiveStore = new AsynchronousSearchActiveStore(clusterService);
        this.asynchronousSearchService = new AsynchronousSearchService(persistenceService, asynchronousSearchActiveStore, client,
                clusterService,
                threadPool, new InternalAsynchronousSearchStats(), namedWriteableRegistry);
        return Arrays.asList(persistenceService, asynchronousSearchService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(SubmitAsynchronousSearchAction.INSTANCE, TransportSubmitAsynchronousSearchAction.class),
                new ActionHandler<>(AsynchronousSearchStatsAction.INSTANCE, TransportAsynchronousSearchStatsAction.class),
                new ActionHandler<>(GetAsynchronousSearchAction.INSTANCE, TransportGetAsynchronousSearchAction.class),
                new ActionHandler<>(DeleteAsynchronousSearchAction.INSTANCE, TransportDeleteAsynchronousSearchAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                AsynchronousSearchManagementService.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING,
                AsynchronousSearchManagementService.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING,
                AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.MAX_SEARCH_RUNNING_TIME_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING,
                LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestSubmitAsynchronousSearchAction(),
                new RestGetAsynchronousSearchAction(),
                new RestDeleteAsynchronousSearchAction(),
                new RestAsynchronousSearchStatsAction());
    }
}
