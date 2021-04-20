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
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
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
import static com.amazon.opendistroforelasticsearch.search.asynchronous.utils.TestUtils.createClusterService;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.action.ActionListener.wrap;
import static org.opensearch.common.unit.TimeValue.timeValueHours;
import static org.hamcrest.Matchers.greaterThan;

public class AsynchronousSearchServiceTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings;
    private Settings settings;
    private ExecutorBuilder<?> executorBuilder;
    static boolean blockPersistence;
    @Before
    public void createObjects() {
        settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), 10)
                .put(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(), true)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
        blockPersistence = false;
    }

    public void testFindContext() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    executorBuilder);
            ClusterService mockClusterService = createClusterService(settings, testThreadPool, discoveryNode, clusterSettings);
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
                            e -> assertTrue("expected timeout got " + e.getClass(), e instanceof OpenSearchTimeoutException)),
                            updateLatch));
            updateLatch.await();
            fakeClient.releaseBlock();
        } finally {
            ThreadPool.terminate(testThreadPool, 200, TimeUnit.MILLISECONDS);
        }
    }

    public void testUpdateExpirationOnRunningSearch() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
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
