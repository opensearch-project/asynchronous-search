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

package com.amazon.opendistroforelasticsearch.search.asynchronous.service;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.processor.AsynchronousSearchPostProcessor;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.AsynchronousSearchAssertions;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AsynchronousSearchPostProcessorTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                        AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testProcessSearchFailureOnDeletedContext() throws AsynchronousSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);

            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchStateMachine asStateMachine = asService.getStateMachine();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            AsynchronousSearchPostProcessor postProcessor = new AsynchronousSearchPostProcessor(persistenceService,
                    asActiveStore, asStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool, clusterService);
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext) asService.createAndStoreContext(
                    submitAsynchronousSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new RuntimeException("runtime-exception"));
            SearchPhaseExecutionException exception = new SearchPhaseExecutionException("phase", "msg", new NullPointerException(),
                    new ShardSearchFailure[]{shardSearchFailure});
            asStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap())));
            asStateMachine.trigger(new SearchDeletedEvent(context));
            AsynchronousSearchResponse asResponse = postProcessor.processSearchFailure(exception, context.getContextId());
            assertNull(asResponse.getId());
            assertNull(asResponse.getSearchResponse());
            assertEquals(-1L, asResponse.getExpirationTimeMillis());
            assertEquals(-1L, asResponse.getStartTimeMillis());
            assertEquals(AsynchronousSearchState.FAILED, asResponse.getState());
            assertThat(asResponse.getError(), instanceOf(SearchPhaseExecutionException.class));
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseBeginPersistence() throws AsynchronousSearchStateMachineClosedException, InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchStateMachine asStateMachine = asService.getStateMachine();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            AsynchronousSearchPostProcessor postProcessor = new AsynchronousSearchPostProcessor(persistenceService,
                    asActiveStore, asStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool, clusterService);
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext) asService.createAndStoreContext(
                    submitAsynchronousSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsynchronousSearchResponse asResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asResponse.getId());
            assertNull(asResponse.getError());
            assertEquals(AsynchronousSearchState.PERSISTING, asResponse.getState());
            AsynchronousSearchAssertions.assertSearchResponses(mockSearchResponse, asResponse.getSearchResponse());
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            waitUntil(() -> fakeClient.persistenceCount == 1);
            assertEquals(1, fakeClient.persistenceCount);
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponsePersisted() throws AsynchronousSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchStateMachine asStateMachine = asService.getStateMachine();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            AsynchronousSearchPostProcessor postProcessor = new AsynchronousSearchPostProcessor(persistenceService,
                    asActiveStore, asStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool, clusterService);
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext) asService.createAndStoreContext(
                    submitAsynchronousSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsynchronousSearchResponse asResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asResponse.getId());
            assertNull(asResponse.getError());
            assertEquals(AsynchronousSearchState.PERSISTING, asResponse.getState());
            waitUntil(() -> context.getAsynchronousSearchState() == AsynchronousSearchState.CLOSED);
            assertEquals(AsynchronousSearchState.CLOSED, context.getAsynchronousSearchState());
            AsynchronousSearchAssertions.assertSearchResponses(mockSearchResponse, asResponse.getSearchResponse());
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(1, fakeClient.persistenceCount);
        } catch (InterruptedException e) {
            fail("Test interrupted " + e.getMessage());
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseForExpiredContext() throws AsynchronousSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchStateMachine asStateMachine = asService.getStateMachine();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            AsynchronousSearchPostProcessor postProcessor = new AsynchronousSearchPostProcessor(persistenceService,
                    asActiveStore, asStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool, clusterService);
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueMillis(1));
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext) asService.createAndStoreContext(
                    submitAsynchronousSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            waitUntil(() -> context.isExpired());
            AsynchronousSearchResponse asResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asResponse.getId());
            assertNull(asResponse.getError());
            assertEquals(AsynchronousSearchState.SUCCEEDED, asResponse.getState());
            AsynchronousSearchAssertions.assertSearchResponses(mockSearchResponse, asResponse.getSearchResponse());
            assertTrue(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } catch (InterruptedException e) {
            fail("Interrupted exception" + e.getMessage());
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseOnClosedContext() throws AsynchronousSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchStateMachine asStateMachine = asService.getStateMachine();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            AsynchronousSearchPostProcessor postProcessor = new AsynchronousSearchPostProcessor(persistenceService,
                    asActiveStore, asStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool, clusterService);
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext) asService.createAndStoreContext(
                    submitAsynchronousSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap())));
            asStateMachine.trigger(new SearchDeletedEvent(context));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsynchronousSearchResponse asResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNull(asResponse.getId());
            assertNull(asResponse.getError());
            assertEquals(-1L, asResponse.getExpirationTimeMillis());
            assertEquals(AsynchronousSearchState.SUCCEEDED, asResponse.getState());
            assertEquals(-1L, asResponse.getStartTimeMillis());
            AsynchronousSearchAssertions.assertSearchResponses(mockSearchResponse, asResponse.getSearchResponse());
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    protected SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(emptyList()),
                new Suggest(emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static class FakeClient extends NoOpClient {

        int persistenceCount;

        FakeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof IndexAction) {
                persistenceCount++;
            }
            listener.onResponse(null);
        }
    }
}
