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

package com.amazon.opendistroforelasticsearch.search.async.service;


import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.DELETED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.hamcrest.Matchers.greaterThan;

public class AsyncSearchServiceTests extends ESTestCase {

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

    public void testFindContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            User user2 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
            assertNull(asyncSearchActiveContext.getTask());
            assertNull(asyncSearchActiveContext.getAsyncSearchId());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
            assertEquals(asyncSearchActiveContext.getUser(), user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(asyncSearchActiveContext.getTask(), task);
            assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asyncSearchActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
            CountDownLatch findContextLatch = new CountDownLatch(3);
            ActionListener<AsyncSearchContext> expectedSuccessfulActive = new LatchedActionListener<>(wrap(
                    r -> {
                            assertTrue(r instanceof AsyncSearchActiveContext);
                            assertEquals(r, context);
                    }, e -> fail("Find context shouldn't have failed. " + e.getMessage())), findContextLatch);
            ActionListener<AsyncSearchContext> expectedSecurityException = new LatchedActionListener<>(wrap(
                    r -> fail("Expecting security exception"), e -> assertTrue(e instanceof ElasticsearchSecurityException)
            ), findContextLatch);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(),
                    asyncSearchActiveContext.getContextId(), user1, expectedSuccessfulActive);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(),
                    asyncSearchActiveContext.getContextId(), user2, expectedSecurityException);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(),
                    asyncSearchActiveContext.getContextId(), null, expectedSuccessfulActive);

            findContextLatch.await();

            AsyncSearchProgressListener asyncSearchProgressListener = asyncSearchActiveContext.getAsyncSearchProgressListener();
            boolean success = randomBoolean();
            if (success) { //successful search response
                asyncSearchProgressListener.onResponse(getMockSearchResponse());
            } else { // exception occurred in search
                asyncSearchProgressListener.onFailure(new RuntimeException("test"));
            }
            waitUntil(() -> asyncSearchService.getAllActiveContexts().isEmpty());
            if (keepOnCompletion) { //persist to disk
                assertEquals(1, fakeClient.persistenceCount.intValue());
            } else {
                assertEquals(fakeClient.persistenceCount, Integer.valueOf(0));
                CountDownLatch freeContextLatch = new CountDownLatch(1);
                asyncSearchService.findContext(context.getAsyncSearchId(), context.getContextId(), null,
                        new LatchedActionListener<>(wrap(
                        r -> fail("No context should have been found but found " + asyncSearchService.getAllActiveContexts().size()),
                        e -> assertTrue(e instanceof ResourceNotFoundException)), freeContextLatch));
                freeContextLatch.await();
            }
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testUpdateExpirationOnRunningSearch() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = false;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, null);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
            assertNull(asyncSearchActiveContext.getTask());
            assertNull(asyncSearchActiveContext.getAsyncSearchId());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });

            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(asyncSearchActiveContext.getTask(), task);
            assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
            long originalExpirationTimeMillis = asyncSearchActiveContext.getExpirationTimeMillis();
            assertEquals(originalExpirationTimeMillis, task.getStartTime() + keepAlive.millis());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
            CountDownLatch findContextLatch = new CountDownLatch(1);
            asyncSearchService.findContext(asyncSearchActiveContext.getAsyncSearchId(), asyncSearchActiveContext.getContextId(), null,
                    new LatchedActionListener<>(wrap(
                    r -> {
                        assertTrue(r instanceof AsyncSearchActiveContext);
                        assertEquals(r, context);
                    }, e -> fail("Find context shouldn't have failed")
            ), findContextLatch));
            findContextLatch.await();
            CountDownLatch updateLatch = new CountDownLatch(1);
            TimeValue newKeepAlive = timeValueDays(10);
            asyncSearchService.updateKeepAliveAndGetContext(asyncSearchActiveContext.getAsyncSearchId(), newKeepAlive,
                    asyncSearchActiveContext.getContextId(), null, new LatchedActionListener<>(wrap(r -> {
                            assertTrue(r instanceof AsyncSearchActiveContext);
                            assertThat(r.getExpirationTimeMillis(), greaterThan(originalExpirationTimeMillis));
                    }, e -> fail()), updateLatch));
            updateLatch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testFindContextOnNonExistentSearch() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            User user1 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(false);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            CountDownLatch findContextLatch = new CountDownLatch(2);
            ActionListener<AsyncSearchContext> failureExpectingListener = new LatchedActionListener<>(wrap(r -> fail(),
                    e -> assertTrue(e instanceof ResourceNotFoundException)), findContextLatch);
            asyncSearchService.findContext("nonExistentId", new AsyncSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong()),
                    null, failureExpectingListener);
            asyncSearchService.findContext("nonExistentId", new AsyncSearchContextId(randomAlphaOfLength(10), randomNonNegativeLong()),
                    user1, failureExpectingListener);
            findContextLatch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testBootStrapOnClosedSearch() {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = false;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, null);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
            assertNull(asyncSearchActiveContext.getTask());
            assertNull(asyncSearchActiveContext.getAsyncSearchId());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);

            //close context
            asyncSearchActiveContext.close();
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchActiveContext.setState(DELETED);
            expectThrows(IllegalStateException.class, () -> asyncSearchService.bootstrapSearch(task,
                    context.getContextId()));
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testFreeActiveContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            User user2 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
            assertNull(asyncSearchActiveContext.getTask());
            assertNull(asyncSearchActiveContext.getAsyncSearchId());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
            assertEquals(asyncSearchActiveContext.getUser(), user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(asyncSearchActiveContext.getTask(), task);
            assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asyncSearchActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
            assertTrue(asyncSearchService.freeActiveContext(asyncSearchActiveContext));
            assertTrue(asyncSearchService.getAllActiveContexts().isEmpty());
            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(context.getAsyncSearchId(), context.getContextId(), user1,
                    new LatchedActionListener<>(wrap(r -> fail(), e -> assertTrue(e instanceof ResourceNotFoundException)), latch));
            latch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testFreeContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            User user2 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchContext context = asyncSearchService.createAndStoreContext(submitAsyncSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsyncSearchActiveContext);
            AsyncSearchActiveContext asyncSearchActiveContext = (AsyncSearchActiveContext) context;
            assertNull(asyncSearchActiveContext.getTask());
            assertNull(asyncSearchActiveContext.getAsyncSearchId());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), INIT);
            assertEquals(asyncSearchActiveContext.getUser(), user1);
            //bootstrap search
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsyncSearchActiveContext) context, null, (c) -> {
            });
            asyncSearchService.bootstrapSearch(task, context.getContextId());
            assertEquals(asyncSearchActiveContext.getTask(), task);
            assertEquals(asyncSearchActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asyncSearchActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asyncSearchActiveContext.getAsyncSearchState(), RUNNING);
            CountDownLatch latch = new CountDownLatch(1);
            asyncSearchService.freeContext(context.getAsyncSearchId(), context.getContextId(), user1,
                    new LatchedActionListener<>(wrap(r -> assertTrue(r), e -> fail()), latch));
            latch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    private static class FakeClient extends NoOpClient {

        Integer persistenceCount;

        FakeClient(ThreadPool threadPool) {
            super(threadPool);
            persistenceCount = 0;
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

    protected SearchResponse getMockSearchResponse() {
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
}
