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


import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.task.AsynchronousSearchTask;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.TestClientUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
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
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.RUNNING;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.hamcrest.Matchers.greaterThan;

public class AsynchronousSearchServiceTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    static boolean blockPersistence;
    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsynchronousSearchActiveStore.MAX_RUNNING_SEARCHES_SETTING.getKey(), 10)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsynchronousSearchActiveStore.MAX_RUNNING_SEARCHES_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
        blockPersistence = false;
    }

    public void testFindContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
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

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            User user2 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);
            assertEquals(asActiveContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {});
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(asActiveContext.getTask(), task);
            assertEquals(asActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asActiveContext.getAsynchronousSearchState(), RUNNING);
            CountDownLatch findContextLatch = new CountDownLatch(3);
            ActionListener<AsynchronousSearchContext> expectedSuccessfulActive = new LatchedActionListener<>(wrap(
                    r -> {
                        assertTrue(r instanceof AsynchronousSearchActiveContext);
                        assertEquals(r, context);
                    }, e -> fail("Find context shouldn't have failed. " + e.getMessage())), findContextLatch);
            ActionListener<AsynchronousSearchContext> expectedSecurityException = new LatchedActionListener<>(wrap(
                    r -> fail("Expecting security exception"), e -> assertTrue(e instanceof ResourceNotFoundException)
            ), findContextLatch);
            asService.findContext(asActiveContext.getAsynchronousSearchId(),
                    asActiveContext.getContextId(), user1, expectedSuccessfulActive);
            asService.findContext(asActiveContext.getAsynchronousSearchId(),
                    asActiveContext.getContextId(), user2, expectedSecurityException);
            asService.findContext(asActiveContext.getAsynchronousSearchId(),
                    asActiveContext.getContextId(), null, expectedSuccessfulActive);

            findContextLatch.await();

            AsynchronousSearchProgressListener asProgressListener = asActiveContext.getAsynchronousSearchProgressListener();
            boolean success = randomBoolean();
            if (success) { //successful search response
                asProgressListener.onResponse(getMockSearchResponse());
            } else { // exception occurred in search
                asProgressListener.onFailure(new RuntimeException("test"));
            }
            waitUntil(() -> asService.getAllActiveContexts().isEmpty());
            if (keepOnCompletion) { //persist to disk
                assertEquals(1, fakeClient.persistenceCount.intValue());
            } else {
                assertEquals(fakeClient.persistenceCount, Integer.valueOf(0));
                CountDownLatch freeContextLatch = new CountDownLatch(1);
                asService.findContext(context.getAsynchronousSearchId(), context.getContextId(), null,
                        new LatchedActionListener<>(wrap(
                                r -> fail("No context should have been found but found " +
                                        asService.getAllActiveContexts().size()),
                                e -> assertTrue(e instanceof ResourceNotFoundException)), freeContextLatch));
                freeContextLatch.await();
            }
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testUpdateExpirationTimesOutBlockedOnPersistence() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode,
                    clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);

            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, null);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {});

            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(asActiveContext.getTask(), task);
            assertEquals(asActiveContext.getStartTimeMillis(), task.getStartTime());
            long originalExpirationTimeMillis = asActiveContext.getExpirationTimeMillis();
            assertEquals(originalExpirationTimeMillis, task.getStartTime() + keepAlive.millis());
            assertEquals(asActiveContext.getAsynchronousSearchState(), RUNNING);
            blockPersistence = true;
            context.getAsynchronousSearchProgressListener().onResponse(getMockSearchResponse());
            CountDownLatch updateLatch = new CountDownLatch(1);
            TimeValue newKeepAlive = timeValueHours(10);
            fakeClient.awaitBlock();
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), newKeepAlive,
                    asActiveContext.getContextId(), null, new LatchedActionListener<>(wrap(r -> fail("expected update req to timeout"),
                            e -> assertTrue("expected timeout got " + e.getClass(), e instanceof ElasticsearchTimeoutException)),
                            updateLatch));
            updateLatch.await();
            fakeClient.releaseBlock();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testUpdateExpirationOnRunningSearch() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode,
                    clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = false;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, null);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {});

            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(asActiveContext.getTask(), task);
            assertEquals(asActiveContext.getStartTimeMillis(), task.getStartTime());
            long originalExpirationTimeMillis = asActiveContext.getExpirationTimeMillis();
            assertEquals(originalExpirationTimeMillis, task.getStartTime() + keepAlive.millis());
            assertEquals(asActiveContext.getAsynchronousSearchState(), RUNNING);
            CountDownLatch findContextLatch = new CountDownLatch(1);
            asService.findContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(), null,
                    new LatchedActionListener<>(wrap(
                            r -> {
                                assertTrue(r instanceof AsynchronousSearchActiveContext);
                                assertEquals(r, context);
                            }, e -> fail("Find context shouldn't have failed")
                    ), findContextLatch));
            findContextLatch.await();
            CountDownLatch updateLatch = new CountDownLatch(1);
            TimeValue newKeepAlive = timeValueHours(10);
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), newKeepAlive,
                    asActiveContext.getContextId(), null, new LatchedActionListener<>(wrap(r -> {
                        assertTrue(r instanceof AsynchronousSearchActiveContext);
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
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            User user1 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(false);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            CountDownLatch findContextLatch = new CountDownLatch(2);
            ActionListener<AsynchronousSearchContext> failureExpectingListener = new LatchedActionListener<>(wrap(r -> fail(),
                    e -> assertTrue(e instanceof ResourceNotFoundException)), findContextLatch);
            asService.findContext("nonExistentId", new AsynchronousSearchContextId(randomAlphaOfLength(10),
                    randomNonNegativeLong()), null, failureExpectingListener);
            asService.findContext("nonExistentId", new AsynchronousSearchContextId(randomAlphaOfLength(10),
                    randomNonNegativeLong()), user1, failureExpectingListener);
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
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = false;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, null);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);

            //close context
            asActiveContext.close();
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {});
            asActiveContext.setState(CLOSED);
            expectThrows(IllegalStateException.class, () -> asService.bootstrapSearch(task,
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
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);
            assertEquals(asActiveContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {}) {
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(asActiveContext.getTask(), task);
            assertEquals(asActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asActiveContext.getAsynchronousSearchState(), RUNNING);
            assertTrue(asService.freeActiveContext(asActiveContext));
            assertTrue(asService.getAllActiveContexts().isEmpty());
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(context.getAsynchronousSearchId(), context.getContextId(), user1,
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

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext asActiveContext = (AsynchronousSearchActiveContext) context;
            assertNull(asActiveContext.getTask());
            assertNull(asActiveContext.getAsynchronousSearchId());
            assertEquals(asActiveContext.getAsynchronousSearchState(), INIT);
            assertEquals(asActiveContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {}) {
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(asActiveContext.getTask(), task);
            assertEquals(asActiveContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(asActiveContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(asActiveContext.getAsynchronousSearchState(), RUNNING);
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(context.getAsynchronousSearchId(), context.getContextId(), user1,
                    new LatchedActionListener<>(wrap(Assert::assertTrue, e -> fail()), latch));
            latch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testFindContextsToReap() {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder) {
                @Override
                public long absoluteTimeInMillis() { // simulate search has over run)
                    return System.currentTimeMillis() - 24 * 3600 * 1000;
                }
            };

            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = TestClientUtils.randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(asService.getContextsToReap().contains(context));
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    private static class FakeClient extends NoOpClient {

        Integer persistenceCount;
        Boolean block;

        FakeClient(ThreadPool threadPool) {
            super(threadPool);
            persistenceCount = 0;
            block = false;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof IndexAction) {
                persistenceCount++;
                if (blockPersistence) {
                    try {
                        block = true;
                        waitUntil(() -> block == false);
                    } catch (InterruptedException e) {
                        logger.error("block failed due to " + e.getMessage());
                    }
                }
            }
            listener.onResponse(null);
        }
        public void awaitBlock() throws InterruptedException {
            waitUntil(() -> block);
        }

        public void releaseBlock() {
            block = false;
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
