package com.amazon.opendistroforelasticsearch.search.async.service;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchTestCase.mockAsyncSearchProgressListener;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
import static com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService.EXPIRATION_TIME_MILLIS;
import static com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService.START_TIME_MILLIS;
import static com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils.randomUser;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncSearchServiceUpdateContextTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private static boolean simulateIsAlive;
    private static boolean simulateTimedOut = false;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsyncSearchActiveStore.MAX_RUNNING_SEARCHES_SETTING.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsyncSearchActiveStore.MAX_RUNNING_SEARCHES_SETTING,
                        AsyncSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsyncSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testUpdateContextWhenContextCloseAndKeepOnCompletionTrue() throws InterruptedException, IOException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore mockStore = mock(AsyncSearchActiveStore.class);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            MockAsyncSearchActiveContext asyncSearchActiveContext = new MockAsyncSearchActiveContext(asyncSearchContextId,
                    discoveryNode.getId(), keepAlive,
                    true, testThreadPool, testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, user1);

            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), asyncSearchActiveContext, null, (c) -> {
            });
            asyncSearchActiveContext.setTask(task);
            long oldExpirationTimeMillis = asyncSearchActiveContext.getExpirationTimeMillis();
            simulateIsAlive = false;
            simulateTimedOut = false;
            CountDownLatch updateLatch = new CountDownLatch(1);
            when(mockStore.getContext(asyncSearchContextId)).thenReturn(Optional.of(asyncSearchActiveContext));
            asyncSearchService.updateKeepAliveAndGetContext(asyncSearchActiveContext.getAsyncSearchId(), keepAlive,
                    asyncSearchActiveContext.getContextId(),
                    user1,
                    new LatchedActionListener<>(wrap(
                            r -> {
                                assertTrue(r instanceof AsyncSearchPersistenceContext);
                                //assert active context expiration time is not updated
                                assertEquals(asyncSearchActiveContext.getExpirationTimeMillis(), oldExpirationTimeMillis);
                            },
                            e -> {
                                fail("expected successful update got " + e.getMessage());
                            }
                    ), updateLatch));
            updateLatch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);

        }
    }

    public void testUpdateContextTimedOut() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore mockStore = mock(AsyncSearchActiveStore.class);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(testThreadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            MockAsyncSearchActiveContext asyncSearchActiveContext = new MockAsyncSearchActiveContext(asyncSearchContextId,
                    discoveryNode.getId(), keepAlive,
                    true, testThreadPool, testThreadPool::absoluteTimeInMillis, asyncSearchProgressListener, user1);

            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), asyncSearchActiveContext, null, (c) -> {
            });
            asyncSearchActiveContext.setTask(task);
            simulateIsAlive = true;
            simulateTimedOut = true;
            when(mockStore.getContext(asyncSearchContextId)).thenReturn(Optional.of(asyncSearchActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.updateKeepAliveAndGetContext(asyncSearchActiveContext.getAsyncSearchId(), timeValueHours(9),
                    asyncSearchActiveContext.getContextId(), randomUser(), new LatchedActionListener<>(
                            wrap(r -> fail("expected timedout exception"),
                                    e -> assertTrue(e instanceof ElasticsearchTimeoutException)), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateActiveContextInvalidUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext activeContext = (AsyncSearchActiveContext) context;
            assertNull(activeContext.getTask());
            assertNull(activeContext.getAsyncSearchId());
            assertEquals(activeContext.getAsyncSearchState(), INIT);
            assertEquals(activeContext.getUser(), user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(activeContext.getTask(), task);
            assertEquals(activeContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(activeContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(activeContext.getAsyncSearchState(), RUNNING);
            CountDownLatch latch = new CountDownLatch(1);
            User differenteUser = randomUser();
            asyncSearchService.updateKeepAliveAndGetContext(context.getAsyncSearchId(), timeValueHours(9), context.getContextId(),
                    differenteUser, new LatchedActionListener<>(wrap(r -> fail("expected security exception Users must be different, " +
                                    "actual user " + user1 + " random user " + differenteUser),
                            e -> {
                                assertTrue(e instanceof ResourceNotFoundException);
                            }), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateClosedContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsyncSearchActiveStore asyncSearchActiveStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchPersistenceService persistenceService = new AsyncSearchPersistenceService(mockClient, mockClusterService,
                    testThreadPool);
            AsyncSearchService asyncSearchService = new AsyncSearchService(persistenceService, asyncSearchActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsyncSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext activeContext = (AsyncSearchActiveContext) context;
            assertNull(activeContext.getTask());
            assertNull(activeContext.getAsyncSearchId());
            assertEquals(activeContext.getAsyncSearchState(), INIT);
            assertEquals(activeContext.getUser(), user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(activeContext.getTask(), task);
            assertEquals(activeContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(activeContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(activeContext.getAsyncSearchState(), RUNNING);
            context.setState(CLOSED);
            ((AsyncSearchActiveContext) context).close();
            CountDownLatch latch = new CountDownLatch(1);

            asyncSearchService.updateKeepAliveAndGetContext(context.getAsyncSearchId(), timeValueHours(9), context.getContextId(),
                    user1, new LatchedActionListener<>(wrap(r -> {
                                if (keepOnCompletion) {
                                    assertTrue(r instanceof AsyncSearchPersistenceContext);
                                } else {
                                    fail("expected resource not found exception, got result.");
                                }
                            },
                            e -> {
                                if (keepOnCompletion) {
                                    fail("expected resource not found exception, got result");
                                } else {
                                    assertTrue(e instanceof ResourceNotFoundException);
                                }
                            }), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    private static class MockClient extends NoOpClient {

        Integer persistenceCount;
        Integer updateCount;

        MockClient(ThreadPool threadPool) {
            super(threadPool);
            persistenceCount = 0;
            updateCount = 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof IndexAction) {
                persistenceCount++;
                listener.onResponse(null);
            } else if (action instanceof UpdateAction) {
                updateCount++;
                ShardId shardId = new ShardId(new Index(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);

                UpdateResponse updateResponse = new UpdateResponse(shardId, "testType", "testId", 1L, 1L, 1L,
                        DocWriteResponse.Result.UPDATED);
                try {
                    Map<String, Object> sourceMap = new HashMap<>();
                    sourceMap.put(EXPIRATION_TIME_MILLIS, randomNonNegativeLong());
                    sourceMap.put(START_TIME_MILLIS, randomNonNegativeLong());
                    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                    builder.map(sourceMap);
                    BytesReference source = BytesReference.bytes(builder);
                    updateResponse.setGetResult(new GetResult(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                            "testType", "testId", 1L, 1L, 1L,
                            true, source, emptyMap(), null));
                    listener.onResponse((Response) updateResponse);
                } catch (IOException e) {
                    fail("Fake client failed to build mock update response");
                }

            } else {
                listener.onResponse(null);
            }
        }
    }

    static class MockAsyncSearchActiveContext extends AsyncSearchActiveContext {
        MockAsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive,
                                     boolean keepOnCompletion, ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                     AsyncSearchProgressListener searchProgressActionListener, User user) {
            super(asyncSearchContextId, nodeId, keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, searchProgressActionListener,
                    user);
        }

        @Override
        public boolean isAlive() {
            if (Thread.currentThread().getName().contains(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME)) {
                return simulateIsAlive;
            }
            return super.isAlive();
        }

        @Override
        public void acquireContextPermitIfRequired(ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
            if (simulateTimedOut) {
                onPermitAcquired.onFailure(new TimeoutException());
            } else {
                super.acquireContextPermitIfRequired(onPermitAcquired, timeout, reason);
            }
        }
    }

    public static SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private ClusterService getClusterService(DiscoveryNode discoveryNode, ThreadPool testThreadPool) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
        ClusterServiceUtils.setState(clusterService,
                ClusterStateCreationUtils.stateWithActivePrimary(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                        true, randomInt(5)));
        return clusterService;
    }

}
