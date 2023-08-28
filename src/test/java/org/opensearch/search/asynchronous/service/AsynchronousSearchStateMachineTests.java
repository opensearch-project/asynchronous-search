/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.service;

import org.opensearch.search.asynchronous.commons.AsynchronousSearchTestCase;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchStateMachineClosedException;
import org.opensearch.search.asynchronous.context.state.event.BeginPersistEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchFailureEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchStartedEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchSuccessfulEvent;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
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
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.FAILED;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.PERSISTING;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.RUNNING;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.SUCCEEDED;
import static org.opensearch.search.asynchronous.utils.TestUtils.createClusterService;

public class AsynchronousSearchStateMachineTests extends AsynchronousSearchTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private Settings settings;

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
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testStateMachine() throws InterruptedException, BrokenBarrierException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test", executorBuilder);
            ClusterService mockClusterService = createClusterService(settings, threadPool, discoveryNode, clusterSettings);
            FakeClient fakeClient = new FakeClient(threadPool);
            AsynchronousSearchActiveStore asActiveStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(fakeClient,
                    mockClusterService,
                    threadPool);
            CustomContextListener customContextListener = new CustomContextListener();
            AsynchronousSearchService asService = new AsynchronousSearchService(persistenceService, asActiveStore, fakeClient,
                    mockClusterService, threadPool, customContextListener, new NamedWriteableRegistry(emptyList()));
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(threadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, discoveryNode.getId(),
                    keepAlive, true, threadPool,
                    threadPool::absoluteTimeInMillis, asProgressListener, null, () -> true);
            assertNull(context.getTask());
            assertEquals(context.getAsynchronousSearchState(), INIT);
            AsynchronousSearchStateMachine stateMachine = asService.getStateMachine();
            AtomicInteger numCompleted = new AtomicInteger();
            AtomicInteger numFailure = new AtomicInteger();

            doConcurrentStateMachineTrigger(stateMachine, new SearchStartedEvent(context, new AsynchronousSearchTask(
                    randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null,
                            (a) -> {})),
                    RUNNING, IllegalStateException.class, Optional.empty());
            boolean success = randomBoolean();
            assertNotNull(context.getTask());
            if (randomBoolean()) { //delete running context
                doConcurrentStateMachineTrigger(stateMachine, new SearchDeletedEvent(context), CLOSED,
                        AsynchronousSearchStateMachineClosedException.class, Optional.empty());
            } else {
                if (success) {
                    doConcurrentStateMachineTrigger(stateMachine, new SearchFailureEvent(context, new RuntimeException("test")), FAILED,
                            IllegalStateException.class, Optional.empty());
                    numFailure.getAndIncrement();
                } else {//success or failure
                    doConcurrentStateMachineTrigger(stateMachine, new SearchSuccessfulEvent(context, getMockSearchResponse()), SUCCEEDED,
                            IllegalStateException.class, Optional.empty());
                    numCompleted.getAndIncrement();
                }
                doConcurrentStateMachineTrigger(stateMachine, new BeginPersistEvent(context), PERSISTING,
                        IllegalStateException.class, Optional.of(AsynchronousSearchStateMachineClosedException.class));
                waitUntil(() -> context.getAsynchronousSearchState().equals(CLOSED), 1, TimeUnit.MINUTES);
                assertTrue(context.getAsynchronousSearchState().toString() + " numFailure : " + numFailure.get() + " numSuccess : "
                                + numCompleted.get(),
                        context.getAsynchronousSearchState().equals(CLOSED));
                assertEquals(1, customContextListener.getPersistedCount() + customContextListener.getPersistFailedCount());
            }
            assertEquals(numCompleted.get(), customContextListener.getCompletedCount());
            assertEquals(numFailure.get(), customContextListener.getFailedCount());
            assertEquals("success:" + success, 0, customContextListener.getRunningCount());
            assertEquals(1, customContextListener.getDeletedCount());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    private SearchResponse getMockSearchResponse() {
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


    private <T extends Exception, R extends Exception> void doConcurrentStateMachineTrigger(
            AsynchronousSearchStateMachine asStateMachine, AsynchronousSearchContextEvent event, AsynchronousSearchState finalState,
            Class<T> throwable, Optional<Class<R>> terminalStateException) throws InterruptedException, BrokenBarrierException {
        int numThreads = 10;
        List<Thread> operationThreads = new ArrayList<>();
        AtomicInteger numTriggerSuccess = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    AsynchronousSearchState state = asStateMachine.trigger(event);
                    assertEquals(state, finalState);
                    numTriggerSuccess.getAndIncrement();
                } catch (Exception e) {
                    if (terminalStateException.isPresent()) {
                        assertTrue(terminalStateException.get().isInstance(e) || throwable.isInstance(e));
                    } else {
                        assertTrue(throwable.isInstance(e));
                    }
                } finally {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        fail("stage advancement failure");
                    }
                }
            });
            operationThreads.add(thread);
        }
        operationThreads.forEach(Thread::start);
        barrier.await();
        for (Thread t : operationThreads) {
            t.join();
        }
        assertEquals(1, numTriggerSuccess.get());
    }

    private static class FakeClient extends NoOpClient {

        FakeClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (action instanceof CreateIndexAction) {
                listener.onResponse(null);
                return;
            }
            if (randomBoolean()) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new OpenSearchException(new RuntimeException("test")));
            }
        }
    }

    static class CustomContextListener implements AsynchronousSearchContextEventListener {

        private final AtomicInteger runningCount = new AtomicInteger();
        private final AtomicInteger persistedCount = new AtomicInteger();
        private final AtomicInteger persistFailedCount = new AtomicInteger();
        private final AtomicInteger completedCount = new AtomicInteger();
        private final AtomicInteger failedCount = new AtomicInteger();
        private final AtomicInteger deletedCount = new AtomicInteger();

        @Override
        public void onContextCompleted(AsynchronousSearchContextId contextId) {
            completedCount.getAndIncrement();
            runningCount.getAndDecrement();
        }

        @Override
        public void onContextFailed(AsynchronousSearchContextId contextId) {
            failedCount.getAndIncrement();
            runningCount.getAndDecrement();
        }

        @Override
        public void onContextPersisted(AsynchronousSearchContextId contextId) {
            persistedCount.getAndIncrement();
        }

        @Override
        public void onContextPersistFailed(AsynchronousSearchContextId contextId) {
            persistFailedCount.getAndIncrement();
        }

        @Override
        public void onContextDeleted(AsynchronousSearchContextId contextId) {
            deletedCount.getAndIncrement();
        }

        @Override
        public void onContextRunning(AsynchronousSearchContextId contextId) {
            runningCount.getAndIncrement();
        }

        @Override
        public void onRunningContextDeleted(AsynchronousSearchContextId contextId) {
            runningCount.getAndDecrement();
            deletedCount.getAndIncrement();
        }

        public int getRunningCount() {
            return runningCount.get();
        }

        public int getPersistedCount() {
            return persistedCount.get();
        }

        public int getPersistFailedCount() {
            return persistFailedCount.get();
        }

        public int getCompletedCount() {
            return completedCount.get();
        }

        public int getFailedCount() {
            return failedCount.get();
        }

        public int getDeletedCount() {
            return deletedCount.get();
        }
    }
}
