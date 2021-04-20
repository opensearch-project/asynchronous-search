/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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
