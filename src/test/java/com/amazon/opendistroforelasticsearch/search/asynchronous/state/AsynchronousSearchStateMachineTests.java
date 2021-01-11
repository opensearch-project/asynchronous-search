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

package com.amazon.opendistroforelasticsearch.search.asynchronous.state;

import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.search.SearchAction;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.PERSISTING;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.SUCCEEDED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class AsynchronousSearchStateMachineTests extends AsynchronousSearchTestCase {

    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;

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
                        AsynchronousSearchService.MAX_SEARCH_RUNNING_TIME_SETTING,
                        AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING,
                        AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testStateMachine() throws InterruptedException, BrokenBarrierException {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test", executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
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
                    threadPool::absoluteTimeInMillis, asProgressListener, null);
            assertNull(context.getTask());
            assertEquals(context.getAsynchronousSearchState(), INIT);
            AsynchronousSearchStateMachine stateMachine = asService.getStateMachine();
            AtomicInteger numCompleted = new AtomicInteger();
            AtomicInteger numFailure = new AtomicInteger();

            doConcurrentStateMachineTrigger(stateMachine, new SearchStartedEvent(context, new AsynchronousSearchTask(
                    randomNonNegativeLong(), "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null,
                            (a) -> {})),
                    RUNNING, IllegalStateException.class, Optional.empty());
            assertNotNull(context.getTask());
            if (randomBoolean()) { //delete running context
                doConcurrentStateMachineTrigger(stateMachine, new SearchDeletedEvent(context), CLOSED,
                        AsynchronousSearchStateMachineClosedException.class, Optional.empty());
            } else {
                if (randomBoolean()) {//success or failure
                    doConcurrentStateMachineTrigger(stateMachine, new SearchSuccessfulEvent(context, getMockSearchResponse()), SUCCEEDED,
                            IllegalStateException.class, Optional.empty());
                    numCompleted.getAndIncrement();
                } else {
                    doConcurrentStateMachineTrigger(stateMachine, new SearchFailureEvent(context, new RuntimeException("test")), FAILED,
                            IllegalStateException.class, Optional.empty());
                    numFailure.getAndIncrement();
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
            assertEquals(0, customContextListener.getRunningCount());
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
                listener.onFailure(new ElasticsearchException(new RuntimeException("test")));
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
