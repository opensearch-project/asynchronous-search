/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.service;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import org.opensearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.search.asynchronous.commons.AsynchronousSearchTestCase.mockAsynchronousSearchProgressListener;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.action.ActionListener.wrap;
import static org.opensearch.common.unit.TimeValue.timeValueDays;
import static org.opensearch.common.unit.TimeValue.timeValueHours;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
import static org.opensearch.search.asynchronous.utils.TestClientUtils.randomUser;

public class AsynchronousSearchServiceFreeContextTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private static boolean persisted = false;
    private static boolean userMatches = false;
    private static boolean simulateTimedOut = false;
    private static boolean simulateUncheckedException = false;


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
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
        persisted = false;
        userMatches = false;
        simulateTimedOut = false;
        simulateUncheckedException = false;
    }

    public void testFreePersistedContextUserMatches() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            AsynchronousSearchId asId = new AsynchronousSearchId(discoveryNode.getId(), randomNonNegativeLong(),
                    asContextId);
            persisted = true;
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(AsynchronousSearchIdConverter.buildAsyncId(asId), asContextId, null,
                    new LatchedActionListener<>(ActionListener.wrap(Assert::assertTrue,
                            e -> fail("expected successful delete because persistence is true. but got " + e.getMessage())), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreePersistedContextUserNotMatches() throws InterruptedException {
            DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            AsynchronousSearchId asId = new AsynchronousSearchId(discoveryNode.getId(), randomNonNegativeLong(),
                    asContextId);
            persisted = true;
            userMatches = false;
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(AsynchronousSearchIdConverter.buildAsyncId(asId), asContextId, randomUser(),
                    new LatchedActionListener<>(ActionListener.wrap(
                            r -> {
                                fail("Expected resource_not_found_exception due to user mismatch security exception. received delete " +
                                        "acknowledgement : " + r);

                            }, e -> assertTrue("expected resource_not_found_exception got " + e.getClass().getName(),
                                    e instanceof ResourceNotFoundException)), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextRunningUserMatches() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            AsynchronousSearchActiveContext asActiveContext = new AsynchronousSearchActiveContext(asContextId, discoveryNode.getId(),
                    keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1, () -> true);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {}) {
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
            asActiveContext.setTask(task);
            asActiveContext.setState(AsynchronousSearchState.RUNNING);
            when(mockStore.getContext(any())).thenReturn(Optional.of(asActiveContext));
            persisted = false;
            User user = randomBoolean() ? null : user1; //user
            //task cancellation fails
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    user, new LatchedActionListener<>(ActionListener.wrap(
                            Assert::assertTrue,
                            e -> {
                                fail("active context should have been deleted");
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();


        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeActiveContextWithCancelledTask() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            AsynchronousSearchActiveContext asActiveContext = new AsynchronousSearchActiveContext(asContextId, discoveryNode.getId(),
                    keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1, () -> true);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {
            }) {
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
            asActiveContext.setTask(task);
            asActiveContext.setState(AsynchronousSearchState.RUNNING);
            when(mockStore.getContext(any())).thenReturn(Optional.of(asActiveContext));
            persisted = false;
            //task cancellation fails
            CountDownLatch latch = new CountDownLatch(1);

            User user = randomBoolean() ? null : user1; //user
            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    user, new LatchedActionListener<>(ActionListener.wrap(
                            Assert::assertTrue,
                            e -> {
                                fail("active context should have been deleted");
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();


        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextUserNotMatches() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService,
                    new AsynchronousSearchActiveStore(mockClusterService), mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchActiveContext context = (AsynchronousSearchActiveContext)
                    asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(), () -> null, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(),
                    "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null, (c) -> {});

            asService.bootstrapSearch(task, context.getContextId());
            persisted = false;
            CountDownLatch latch = new CountDownLatch(1);
            User user2 = randomUser();
            asService.freeContext(context.getAsynchronousSearchId(), context.getContextId(),
                    user2, new LatchedActionListener<>(ActionListener.wrap(
                            r -> {
                                fail("expected security exception but got delete ack " + r + " search creator " + user1 + " " +
                                        "accessing user " + user2);
                            },
                            e -> {
                                assertTrue(e.getClass().getName(), e instanceof ResourceNotFoundException);
                            }
                    ), latch));
            latch.await();
            mockClusterService.stop();


        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextTimedOut() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            MockAsynchronousSearchActiveContext asActiveContext = new MockAsynchronousSearchActiveContext(asContextId,
                    discoveryNode.getId(), keepAlive,
                    true, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {});
            asActiveContext.setTask(task);
            simulateTimedOut = true;
            persisted = true;
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    null, new LatchedActionListener<>(
                            wrap(r -> fail("expected timedout exception"),
                                    e -> assertTrue(e instanceof OpenSearchTimeoutException)), latch));
            latch.await();
            assertEquals(0, (int) mockClient.deleteCount);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextPermitAcquisitionFailure() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            MockAsynchronousSearchActiveContext asActiveContext = new MockAsynchronousSearchActiveContext(asContextId,
                    discoveryNode.getId(), keepAlive,
                    true, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {});
            asActiveContext.setTask(task);
            simulateUncheckedException = true;
            persisted = false;
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    null, new LatchedActionListener<>(
                            wrap(r -> fail("Expected resource_not_found_exception. Got acknowledgement " + r),
                                    e -> {
                                        assertTrue(e.getClass().getName(), e instanceof ResourceNotFoundException);
                                    }), latch));
            latch.await();
            assertEquals(1, (int) mockClient.deleteCount);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeContextPermitAcquisitionFailureKeepOnCompletionFalse() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService,
                    testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            MockAsynchronousSearchActiveContext asActiveContext = new MockAsynchronousSearchActiveContext(asContextId,
                    discoveryNode.getId(), keepAlive,
                    false, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {
            });
            asActiveContext.setTask(task);
            simulateUncheckedException = true;
            persisted = false;
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    null, new LatchedActionListener<>(
                            wrap(Assert::assertFalse,
                                    e -> {
                                        fail("Expected acknowledgement false. Got error " + e.getMessage());
                                    }), latch));
            latch.await();
            assertEquals(0, (int) mockClient.deleteCount);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testFreeActiveContextKeepOnCompletionFalse() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore mockStore = mock(AsynchronousSearchActiveStore.class);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, mockStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueDays(9);
            boolean keepOnCompletion = true;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            AsynchronousSearchActiveContext asActiveContext = new AsynchronousSearchActiveContext(asContextId, discoveryNode.getId(),
                    keepAlive, false, testThreadPool, testThreadPool::absoluteTimeInMillis, asProgressListener, user1, () -> true);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID,
                    emptyMap(), asActiveContext, null, (c) -> {}) {
                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
            asActiveContext.setTask(task);
            asActiveContext.setState(AsynchronousSearchState.RUNNING);
            when(mockStore.getContext(any())).thenReturn(Optional.of(asActiveContext));
            persisted = false;
            //task cancellation fails
            User user = randomBoolean() ? null : user1; //user
            CountDownLatch latch = new CountDownLatch(1);

            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    user, new LatchedActionListener<>(ActionListener.wrap(
                            Assert::assertTrue,
                            e -> {
                                fail("active context should have been deleted");
                            }
                    ), latch));
            latch.await();

            CountDownLatch latch1 = new CountDownLatch(1);

            asService.freeContext(asActiveContext.getAsynchronousSearchId(), asActiveContext.getContextId(),
                    user, new LatchedActionListener<>(ActionListener.wrap(
                            r -> {
                                fail("Expected resource_not_found_exception");
                            },
                            e -> {
                                assertTrue(e.getClass().getName(), e instanceof ResourceNotFoundException);
                            }
                    ), latch1));
            latch1.await();
            mockClusterService.stop();


        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }


    private static class MockClient extends NoOpClient {

        Integer persistenceCount;
        Integer deleteCount;

        MockClient(ThreadPool threadPool) {
            super(threadPool);
            persistenceCount = 0;
            deleteCount = 0;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof IndexAction) {
                persistenceCount++;
                listener.onResponse(null);
            } else if (action instanceof UpdateAction) { //even delete is being done by UpdateAction
                deleteCount++;
                ShardId shardId = new ShardId(new Index(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);
                UpdateResponse updateResponse = new UpdateResponse(shardId, "testType", "testId", 1L, 1L, 1L,
                        persisted ? (userMatches ? DocWriteResponse.Result.DELETED : DocWriteResponse.Result.NOOP)
                                : DocWriteResponse.Result.NOT_FOUND);
                listener.onResponse((Response) updateResponse);

            } else if (action instanceof DeleteAction) {
                deleteCount++;
                ShardId shardId = new ShardId(new Index(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);
                DeleteResponse deleteResponse = new DeleteResponse(shardId, "testType", "testId",
                        1L, 1L, 1L, persisted);
                listener.onResponse((Response) deleteResponse);
            } else if (action instanceof CancelTasksAction) {
                    listener.onFailure(new RuntimeException("message"));

            } else {
                listener.onResponse(null);
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
                ClusterStateCreationUtils.stateWithActivePrimary(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX,
                        true, randomInt(5)));
        return clusterService;
    }

    static class MockAsynchronousSearchActiveContext extends AsynchronousSearchActiveContext {
        MockAsynchronousSearchActiveContext(AsynchronousSearchContextId asContextId, String nodeId, TimeValue keepAlive,
                                            boolean keepOnCompletion, ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                            AsynchronousSearchProgressListener searchProgressActionListener, User user) {
            super(asContextId, nodeId, keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, searchProgressActionListener,
                    user, () -> true);
        }


        @Override
        public void acquireContextPermitIfRequired(ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
            if (simulateTimedOut) {
                onPermitAcquired.onFailure(new TimeoutException());
            } else if (simulateUncheckedException) {
                onPermitAcquired.onFailure(new RuntimeException());
            } else {
                super.acquireContextPermitIfRequired(onPermitAcquired, timeout, reason);
            }
        }
    }

}
