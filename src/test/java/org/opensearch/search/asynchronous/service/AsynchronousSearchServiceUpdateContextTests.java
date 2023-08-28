/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.service;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.LatchedActionListener;
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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.index.get.GetResult;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceContext;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import org.opensearch.search.asynchronous.task.AsynchronousSearchTask;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.core.action.ActionListener.wrap;
import static org.opensearch.common.unit.TimeValue.timeValueHours;
import static org.opensearch.search.asynchronous.commons.AsynchronousSearchTestCase.mockAsynchronousSearchProgressListener;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.RUNNING;
import static org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
import static org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX;
import static org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService.EXPIRATION_TIME_MILLIS;
import static org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService.START_TIME_MILLIS;
import static org.opensearch.search.asynchronous.utils.TestClientUtils.randomUser;

public class AsynchronousSearchServiceUpdateContextTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private static boolean simulateIsAlive;
    private static boolean simulateTimedOut = false;
    private static boolean simulateUncheckedException = false;
    private static boolean docNotFound = false;

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
        simulateUncheckedException = false;
        simulateTimedOut = false;
        docNotFound = false;
    }

    public void testUpdateContextWhenContextCloseAndKeepOnCompletionTrue() throws InterruptedException, IOException {
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

            TimeValue keepAlive = timeValueHours(9);
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
                    TaskId.EMPTY_TASK_ID, emptyMap(), asActiveContext, null, (c) -> {
            });
            asActiveContext.setTask(task);
            long oldExpirationTimeMillis = asActiveContext.getExpirationTimeMillis();
            simulateIsAlive = false;
            simulateTimedOut = false;
            CountDownLatch updateLatch = new CountDownLatch(1);
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), keepAlive,
                    asActiveContext.getContextId(),
                    user1,
                    new LatchedActionListener<>(wrap(
                            r -> {
                                assertTrue(r instanceof AsynchronousSearchPersistenceContext);
                                //assert active context expiration time is not updated
                                assertEquals(asActiveContext.getExpirationTimeMillis(), oldExpirationTimeMillis);
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

            TimeValue keepAlive = timeValueHours(9);
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
                    discoveryNode.getId(), keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis,
                    asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), asActiveContext, null, (c) -> {});
            asActiveContext.setTask(task);
            simulateIsAlive = true;
            simulateTimedOut = true;
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), timeValueHours(9),
                    asActiveContext.getContextId(), randomUser(), new LatchedActionListener<>(
                            wrap(r -> fail("expected timedout exception"),
                                    e -> assertTrue(e instanceof OpenSearchTimeoutException)), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateContextPermitAcquisitionFailure() throws InterruptedException {
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

            TimeValue keepAlive = timeValueHours(9);
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
                    discoveryNode.getId(), keepAlive, true, testThreadPool, testThreadPool::absoluteTimeInMillis,
                    asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), asActiveContext, null, (c) -> {
            });
            asActiveContext.setTask(task);
            simulateIsAlive = true;
            simulateUncheckedException = true;
            long oldExpiration = asActiveContext.getExpirationTimeMillis();
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), timeValueHours(9),
                    asActiveContext.getContextId(), randomUser(), new LatchedActionListener<>(
                            wrap(r -> assertEquals("active context should not have been updated on permit acquisition failure",
                                    asActiveContext.getExpirationTimeMillis(), oldExpiration),
                                    e -> fail("expected update to succeed but got " + e.getMessage())), latch));
            latch.await();
            assertEquals("update should have been attempted on index", mockClient.updateCount.intValue(), 1);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateContextPermitAcquisitionFailureKeepOnCompletionFalse() throws InterruptedException {
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

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = false;
            User user1 = randomBoolean() ? randomUser() : null;
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            MockAsynchronousSearchActiveContext asActiveContext = new MockAsynchronousSearchActiveContext(asContextId,
                    discoveryNode.getId(), keepAlive, false, testThreadPool, testThreadPool::absoluteTimeInMillis,
                    asProgressListener, user1);

            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), asActiveContext, null, (c) -> {
            });
            asActiveContext.setTask(task);
            simulateIsAlive = true;
            simulateUncheckedException = true;
            long oldExpiration = asActiveContext.getExpirationTimeMillis();
            when(mockStore.getContext(asContextId)).thenReturn(Optional.of(asActiveContext));
            CountDownLatch latch = new CountDownLatch(1);
            asService.updateKeepAliveAndGetContext(asActiveContext.getAsynchronousSearchId(), timeValueHours(9),
                    asActiveContext.getContextId(), randomUser(), new LatchedActionListener<>(
                            wrap(r -> fail("expected update to fail but"),
                                    e -> assertTrue(e instanceof ResourceNotFoundException)), latch));
            latch.await();
            assertEquals("update should not have been attempted on index", mockClient.updateCount.intValue(), 0);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateRunningContextValidUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext activeContext = (AsynchronousSearchActiveContext) context;
            assertNull(activeContext.getTask());
            assertNull(activeContext.getAsynchronousSearchId());
            assertEquals(activeContext.getAsynchronousSearchState(), INIT);
            assertEquals(activeContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {
            });
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(activeContext.getTask(), task);
            assertEquals(activeContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(activeContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(activeContext.getAsynchronousSearchState(), RUNNING);
            CountDownLatch latch = new CountDownLatch(1);
            Long oldExpiration = context.getExpirationTimeMillis();
            asService.updateKeepAliveAndGetContext(context.getAsynchronousSearchId(), timeValueHours(10), context.getContextId(),
                    user1, new LatchedActionListener<>(wrap(r -> assertThat(context.getExpirationTimeMillis(), greaterThan(oldExpiration)),
                            e -> {
                                fail("Expected successful update but got failure " + e.getMessage());
                            }), latch));
            latch.await();
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateContextNoActiveContextFound() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            String node = UUID.randomUUID().toString();
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(testThreadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            User user = randomUser();
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node,
                    keepAlive, keepOnCompletion, testThreadPool,
                    testThreadPool::absoluteTimeInMillis, asProgressListener, user, () -> true);
            CountDownLatch latch = new CountDownLatch(1);
            docNotFound = true;
            asService.updateKeepAliveAndGetContext(context.getAsynchronousSearchId(), keepAlive, context.getContextId(),
                    user1, new LatchedActionListener<>(wrap(r -> fail("Expected resource_not_found_exception"),
                            e -> assertTrue("Expected resource_not_found_exception but got " + e.getMessage(),
                                    e instanceof ResourceNotFoundException)), latch));
            latch.await();
            assertEquals(mockClient.updateCount.intValue(), 1);
            mockClusterService.stop();
        } finally {
            ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testUpdateActiveContextInvalidUser() throws InterruptedException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = true;
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext activeContext = (AsynchronousSearchActiveContext) context;
            assertNull(activeContext.getTask());
            assertNull(activeContext.getAsynchronousSearchId());
            assertEquals(activeContext.getAsynchronousSearchState(), INIT);
            assertEquals(activeContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {
            });
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(activeContext.getTask(), task);
            assertEquals(activeContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(activeContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(activeContext.getAsynchronousSearchState(), RUNNING);
            CountDownLatch latch = new CountDownLatch(1);
            User differenteUser = randomUser();
            asService.updateKeepAliveAndGetContext(context.getAsynchronousSearchId(), timeValueHours(9), context.getContextId(),
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
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, executorBuilder);
            ClusterService mockClusterService = getClusterService(discoveryNode, testThreadPool);
            MockClient mockClient = new MockClient(testThreadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(mockClient,
                    mockClusterService, testThreadPool);
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, mockClient,
                    mockClusterService, testThreadPool, new InternalAsynchronousSearchStats(), new NamedWriteableRegistry(emptyList()));

            TimeValue keepAlive = timeValueHours(9);
            boolean keepOnCompletion = randomBoolean();
            User user1 = randomUser();
            SearchRequest searchRequest = new SearchRequest();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchContext context = asService.createAndStoreContext(submitAsynchronousSearchRequest, System.currentTimeMillis(),
                    () -> null, user1);
            assertTrue(context instanceof AsynchronousSearchActiveContext);
            AsynchronousSearchActiveContext activeContext = (AsynchronousSearchActiveContext) context;
            assertNull(activeContext.getTask());
            assertNull(activeContext.getAsynchronousSearchId());
            assertEquals(activeContext.getAsynchronousSearchState(), INIT);
            assertEquals(activeContext.getUser(), user1);
            //bootstrap search
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    TaskId.EMPTY_TASK_ID, emptyMap(), (AsynchronousSearchActiveContext) context, null, (c) -> {
            });
            asService.bootstrapSearch(task, context.getContextId());
            assertEquals(activeContext.getTask(), task);
            assertEquals(activeContext.getStartTimeMillis(), task.getStartTime());
            assertEquals(activeContext.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());
            assertEquals(activeContext.getAsynchronousSearchState(), RUNNING);
            context.setState(CLOSED);
            ((AsynchronousSearchActiveContext) context).close();
            CountDownLatch latch = new CountDownLatch(1);

            asService.updateKeepAliveAndGetContext(context.getAsynchronousSearchId(), timeValueHours(9), context.getContextId(),
                    user1, new LatchedActionListener<>(wrap(r -> {
                                if (keepOnCompletion) {
                                    assertTrue(r instanceof AsynchronousSearchPersistenceContext);
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
                ShardId shardId = new ShardId(new Index(ASYNC_SEARCH_RESPONSE_INDEX,
                        UUID.randomUUID().toString()), 1);
                if (docNotFound) {
                    UpdateResponse updateResponse = new UpdateResponse(shardId, "testId", 1L, 1L, 1L,
                            DocWriteResponse.Result.NOT_FOUND);
                    listener.onResponse((Response) updateResponse);
                } else {
                    UpdateResponse updateResponse = new UpdateResponse(shardId, "testId", 1L, 1L, 1L,
                            DocWriteResponse.Result.UPDATED);
                    try {
                        Map<String, Object> sourceMap = new HashMap<>();
                        sourceMap.put(EXPIRATION_TIME_MILLIS, randomNonNegativeLong());
                        sourceMap.put(START_TIME_MILLIS, randomNonNegativeLong());
                        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
                        builder.map(sourceMap);
                        BytesReference source = BytesReference.bytes(builder);
                        updateResponse.setGetResult(new GetResult(ASYNC_SEARCH_RESPONSE_INDEX,
                                "testId", 1L, 1L, 1L,
                                true, source, emptyMap(), null));
                        listener.onResponse((Response) updateResponse);
                    } catch (IOException e) {
                        fail("Fake client failed to build mock update response");
                    }
                }

            } else {
                listener.onResponse(null);
            }
        }
    }

    static class MockAsynchronousSearchActiveContext extends AsynchronousSearchActiveContext {
        MockAsynchronousSearchActiveContext(AsynchronousSearchContextId asContextId, String nodeId, TimeValue keepAlive,
                                            boolean keepOnCompletion, ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                            AsynchronousSearchProgressListener searchProgressActionListener, User user) {
            super(asContextId, nodeId, keepAlive, keepOnCompletion, threadPool, currentTimeSupplier, searchProgressActionListener,
                    user, () -> true);
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
            } else if (simulateUncheckedException) {
                onPermitAcquired.onFailure(new RuntimeException("test"));
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
                ClusterStateCreationUtils.stateWithActivePrimary(ASYNC_SEARCH_RESPONSE_INDEX,
                        true, randomInt(5)));
        return clusterService;
    }

}
