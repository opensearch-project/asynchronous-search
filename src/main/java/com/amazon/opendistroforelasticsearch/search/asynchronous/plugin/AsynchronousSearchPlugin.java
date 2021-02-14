/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.asynchronous.plugin;

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.GetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.management.AsynchronousSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.rest.RestAsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.rest.RestDeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.rest.RestGetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.rest.RestSubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.transport.TransportAsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.transport.TransportDeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.transport.TransportGetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.transport.TransportSubmitAsynchronousSearchAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;


public class AsynchronousSearchPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    public static final String OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME = "opendistro_asynchronous_search_generic";
    public static final String BASE_URI = "/_opendistro/_asynchronous_search";

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
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
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
                AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING
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
