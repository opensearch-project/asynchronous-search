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

package com.amazon.opendistroforelasticsearch.search.async.plugin;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.management.AsyncSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestAsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.rest.RestSubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportAsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportDeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportGetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.transport.TransportSubmitAsyncSearchAction;
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


public class AsyncSearchPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    public static final String OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME = "opendistro_asynchronous_search_generic";
    public static final String BASE_URI = "/_opendistro/_asynchronous_search";

    private AsyncSearchPersistenceService persistenceService;
    private AsyncSearchActiveStore asyncSearchActiveStore;
    private AsyncSearchService asyncSearchService;

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(new SystemIndexDescriptor(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                "Stores the response for async search"));
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(AsyncSearchManagementService.class);
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
        this.persistenceService = new AsyncSearchPersistenceService(client, clusterService, threadPool);
        this.asyncSearchActiveStore = new AsyncSearchActiveStore(clusterService);
        this.asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, client, clusterService,
                threadPool, namedWriteableRegistry);
        return Arrays.asList(persistenceService, asyncSearchService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(SubmitAsyncSearchAction.INSTANCE, TransportSubmitAsyncSearchAction.class),
                new ActionHandler<>(AsyncSearchStatsAction.INSTANCE, TransportAsyncSearchStatsAction.class),
                new ActionHandler<>(GetAsyncSearchAction.INSTANCE, TransportGetAsyncSearchAction.class),
                new ActionHandler<>(DeleteAsyncSearchAction.INSTANCE, TransportDeleteAsyncSearchAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                AsyncSearchActiveStore.MAX_RUNNING_CONTEXT,
                AsyncSearchService.MAX_KEEP_ALIVE_SETTING,
                AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                AsyncSearchManagementService.RESPONSE_CLEAN_UP_INTERVAL_SETTING,
                AsyncSearchManagementService.REAPER_INTERVAL_SETTING
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestSubmitAsyncSearchAction(),
                new RestGetAsyncSearchAction(),
                new RestDeleteAsyncSearchAction(),
                new RestAsyncSearchStatsAction());
    }
}
