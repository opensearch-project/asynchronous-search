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

package com.amazon.opendistroforelasticsearch.search.async.state;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchTransition;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchClosedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;
import static java.util.Collections.emptyMap;

public class AsyncSearchStateMachineTests extends AsyncSearchTestCase {

    private final String node = UUID.randomUUID().toString();

    public void testStateMachine() throws InterruptedException, BrokenBarrierException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(threadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            assertNull(context.getTask());
            assertEquals(context.getAsyncSearchState(), INIT);
            AtomicInteger numPersisted = new AtomicInteger();
            AtomicInteger numPersistFailed = new AtomicInteger();
            AtomicInteger numClosed = new AtomicInteger();
            AtomicInteger numPersisting = new AtomicInteger();
            AsyncSearchStateMachine stateMachine = initStateMachine(numPersisted, numPersistFailed, numClosed,
                    numPersisting);

            doConcurrentStateMachineTrigger(stateMachine, new SearchStartedEvent(context, new AsyncSearchTask(randomNonNegativeLong(),
                            "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null,
                            (a) -> {})),
                    RUNNING, AsyncSearchStateMachineException.class);
            assertNotNull(context.getTask());
            if (randomBoolean()) {//success or failure
                doConcurrentStateMachineTrigger(stateMachine, new SearchSuccessfulEvent(context, getMockSearchResponse()), SUCCEEDED,
                        AsyncSearchStateMachineException.class);
            } else {
                doConcurrentStateMachineTrigger(stateMachine, new SearchFailureEvent(context, new RuntimeException("test")), FAILED,
                        AsyncSearchStateMachineException.class);
            }
            doConcurrentStateMachineTrigger(stateMachine, new BeginPersistEvent(context), PERSISTING,
                    AsyncSearchStateMachineException.class);
            if (randomBoolean()) {
                doConcurrentStateMachineTrigger(stateMachine, new SearchResponsePersistedEvent(context), PERSISTED,
                        AsyncSearchStateMachineException.class);
            } else {
                doConcurrentStateMachineTrigger(stateMachine, new SearchResponsePersistFailedEvent(context), PERSIST_FAILED,
                        AsyncSearchStateMachineException.class);
            }
            doConcurrentStateMachineTrigger(stateMachine, new SearchClosedEvent(context), CLOSED,
                    AsyncSearchStateMachineClosedException.class);
            assertEquals(1, numPersisted.get() + numPersistFailed.get());
            assertEquals(1, numPersisting.get());
            assertEquals(1, numClosed.get());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
    
    private SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private AsyncSearchStateMachine initStateMachine(final AtomicInteger numPersisted, final AtomicInteger numPersistFailed,
                                                     final AtomicInteger numDeleted, final AtomicInteger numPersisting) {
        AsyncSearchStateMachine stateMachine = new AsyncSearchStateMachine(
                EnumSet.allOf(AsyncSearchState.class), INIT);

        stateMachine.markTerminalStates(EnumSet.of(CLOSED));

        stateMachine.registerTransition(new AsyncSearchTransition<>(INIT, RUNNING,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).setTask(e.getSearchTask()),
                (contextId, listener) -> listener.onContextRunning(contextId), SearchStartedEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, SUCCEEDED,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchResponse(e.getSearchResponse()),
                (contextId, listener) -> listener.onContextCompleted(contextId), SearchSuccessfulEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, FAILED,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchFailure(e.getException()),
                (contextId, listener) -> listener.onContextFailed(contextId), SearchFailureEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(SUCCEEDED, PERSISTING,
                (s, e) -> numPersisting.getAndIncrement(),
                (contextId, listener) -> listener.onContextPersisted(contextId), BeginPersistEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(FAILED, PERSISTING,
                (s, e) -> numPersisting.getAndIncrement(),
                (contextId, listener) -> listener.onContextPersisted(contextId), BeginPersistEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(PERSISTING, PERSISTED,
                (s, e) -> numPersisted.getAndIncrement(),
                (contextId, listener) -> listener.onContextPersisted(contextId), SearchResponsePersistedEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(PERSISTING, PERSIST_FAILED,
                (s, e) -> numPersistFailed.getAndIncrement(),
                (contextId, listener) -> listener.onContextPersistFailed(contextId), SearchResponsePersistFailedEvent.class));

        for (AsyncSearchState state : EnumSet.of(PERSISTING, PERSISTED, PERSIST_FAILED, SUCCEEDED, FAILED, INIT, RUNNING)) {
            stateMachine.registerTransition(new AsyncSearchTransition<>(state, CLOSED,
                    (s, e) -> numDeleted.getAndIncrement(),
                    (contextId, listener) -> listener.onContextClosed(contextId), SearchClosedEvent.class));
        }
        return stateMachine;
    }

    private <T extends Throwable> void doConcurrentStateMachineTrigger(
            AsyncSearchStateMachine asyncSearchStateMachine, AsyncSearchContextEvent event, AsyncSearchState finalState,
            Class<T> throwable) throws InterruptedException, BrokenBarrierException {
        int numThreads = 10;
        List<Thread> operationThreads = new ArrayList<>();
        AtomicInteger numTriggerSuccess = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    AsyncSearchState state = asyncSearchStateMachine.trigger(event);
                    assertEquals(state, finalState);
                    numTriggerSuccess.getAndIncrement();
                } catch (Exception e) {
                    assertTrue(throwable.isInstance(e));
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
}
