package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.utils.AsyncSearchAssertions;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchDeletedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
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

public class AsyncSearchPostProcessorTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsyncSearchActiveStore.MAX_RUNNING_CONTEXT,
                        AsyncSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsyncSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testProcessSearchFailureOnDeletedContext() throws AsyncSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);

            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(fakeClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchStateMachine asyncSearchStateMachine = asyncSearchService.getStateMachine();
            AsyncSearchPostProcessor postProcessor = new AsyncSearchPostProcessor(persistenceService,
                    asyncSearchActiveStore, asyncSearchStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool);
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsyncSearchActiveContext context = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(submitAsyncSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new RuntimeException("runtime-exception"));
            SearchPhaseExecutionException exception = new SearchPhaseExecutionException("phase", "msg", new NullPointerException(),
                    new ShardSearchFailure[]{shardSearchFailure});
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap())));
            asyncSearchStateMachine.trigger(new SearchDeletedEvent(context));
            AsyncSearchResponse asyncSearchResponse = postProcessor.processSearchFailure(exception, context.getContextId());
            assertNull(asyncSearchResponse.getId());
            assertNull(asyncSearchResponse.getSearchResponse());
            assertEquals(-1L, asyncSearchResponse.getExpirationTimeMillis());
            assertEquals(-1L, asyncSearchResponse.getStartTimeMillis());
            assertEquals(AsyncSearchState.FAILED, asyncSearchResponse.getState());
            assertThat(asyncSearchResponse.getError(), instanceOf(SearchPhaseExecutionException.class));
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseBeginPersistence() throws AsyncSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(fakeClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchStateMachine asyncSearchStateMachine = asyncSearchService.getStateMachine();
            AsyncSearchPostProcessor postProcessor = new AsyncSearchPostProcessor(persistenceService,
                    asyncSearchActiveStore, asyncSearchStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool);
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsyncSearchActiveContext context = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(submitAsyncSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsyncSearchResponse asyncSearchResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asyncSearchResponse.getId());
            assertNull(asyncSearchResponse.getError());
            assertEquals(AsyncSearchState.PERSISTING, asyncSearchResponse.getState());
            AsyncSearchAssertions.assertSearchResponses(mockSearchResponse, asyncSearchResponse.getSearchResponse());
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponsePersisted() throws AsyncSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(fakeClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchStateMachine asyncSearchStateMachine = asyncSearchService.getStateMachine();
            AsyncSearchPostProcessor postProcessor = new AsyncSearchPostProcessor(persistenceService,
                    asyncSearchActiveStore, asyncSearchStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool);
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsyncSearchActiveContext context = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(submitAsyncSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsyncSearchResponse asyncSearchResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asyncSearchResponse.getId());
            assertNull(asyncSearchResponse.getError());
            assertEquals(AsyncSearchState.PERSISTING, asyncSearchResponse.getState());
            waitUntil(() -> context.getAsyncSearchState() == AsyncSearchState.CLOSED);
            assertEquals(AsyncSearchState.CLOSED, context.getAsyncSearchState());
            AsyncSearchAssertions.assertSearchResponses(mockSearchResponse, asyncSearchResponse.getSearchResponse());
            assertFalse(activeContextCleanUpConsumerInvocation.get());
            assertEquals(1, fakeClient.persistenceCount);
        } catch (InterruptedException e) {
            fail("Test interrupted " + e.getMessage());
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseForExpiredContext() throws AsyncSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(fakeClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchStateMachine asyncSearchStateMachine = asyncSearchService.getStateMachine();
            AsyncSearchPostProcessor postProcessor = new AsyncSearchPostProcessor(persistenceService,
                    asyncSearchActiveStore, asyncSearchStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool);
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueMillis(1));
            AsyncSearchActiveContext context = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(submitAsyncSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap())));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            waitUntil(() -> context.isExpired());
            AsyncSearchResponse asyncSearchResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNotNull(asyncSearchResponse.getId());
            assertNull(asyncSearchResponse.getError());
            assertEquals(AsyncSearchState.SUCCEEDED, asyncSearchResponse.getState());
            AsyncSearchAssertions.assertSearchResponses(mockSearchResponse, asyncSearchResponse.getSearchResponse());
            assertTrue(activeContextCleanUpConsumerInvocation.get());
            assertEquals(0, fakeClient.persistenceCount);
        } catch (InterruptedException e) {
            fail("Interrupted exception" + e.getMessage());
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testProcessSearchResponseOnClosedContext() throws AsyncSearchStateMachineClosedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        AtomicBoolean activeContextCleanUpConsumerInvocation = new AtomicBoolean();
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(fakeClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsyncSearchStateMachine asyncSearchStateMachine = asyncSearchService.getStateMachine();
            AsyncSearchPostProcessor postProcessor = new AsyncSearchPostProcessor(persistenceService,
                    asyncSearchActiveStore, asyncSearchStateMachine,
                    (context) -> activeContextCleanUpConsumerInvocation.compareAndSet(false, true), testThreadPool);
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueHours(1));
            AsyncSearchActiveContext context = (AsyncSearchActiveContext) asyncSearchService.createAndStoreContext(submitAsyncSearchRequest,
                    System.currentTimeMillis(), () -> InternalAggregationTestCase.emptyReduceContextBuilder(), null);
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context,
                    new SearchTask(0, "n/a", "n/a", "test", null, Collections.emptyMap())));
            asyncSearchStateMachine.trigger(new SearchDeletedEvent(context));
            SearchResponse mockSearchResponse = getMockSearchResponse();
            AsyncSearchResponse asyncSearchResponse = postProcessor.processSearchResponse(mockSearchResponse, context.getContextId());
            assertNull(asyncSearchResponse.getId());
            assertNull(asyncSearchResponse.getError());
            assertEquals(-1L, asyncSearchResponse.getExpirationTimeMillis());
            assertEquals(AsyncSearchState.SUCCEEDED, asyncSearchResponse.getState());
            assertEquals(-1L, asyncSearchResponse.getStartTimeMillis());
            AsyncSearchAssertions.assertSearchResponses(mockSearchResponse, asyncSearchResponse.getSearchResponse());
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
