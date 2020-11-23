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

package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchDeletionEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.exception.AsyncSearchStateMachineException;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.DELETED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;

public class AsyncSearchActiveContextIT extends AsyncSearchSingleNodeTestCase {

    public void testAsyncSearchTransitions() {
        TestThreadPool threadPool = null;
        try {
            //get async search state machine definition
            AsyncSearchStateMachine stateMachine = getInstanceFromNode(AsyncSearchStateMachine.class);
            threadPool = new TestThreadPool("test");
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                    threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            String node = UUID.randomUUID().toString();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            //before search task is registered
            assertNull(context.getTask());
            assertNull(context.getAsyncSearchId());
            assertEquals(context.getAsyncSearchState(), AsyncSearchState.INIT);
            //register search task by triggering searchStartedEvent
            SearchTask task = new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    null, null, Collections.emptyMap());
            AsyncSearchState asyncSearchState = stateMachine.trigger(new SearchStartedEvent(context,
                    task));

            assertEquals(context.getAsyncSearchId(), AsyncSearchIdConverter.buildAsyncId(new AsyncSearchId(node, task.getId(),
                    asyncSearchContextId)));
            assertEquals(task, context.getTask());
            assertEquals(asyncSearchState, RUNNING);
            assertEquals(context.getAsyncSearchState(), RUNNING);
            assertEquals(context.getStartTimeMillis(), task.getStartTime());
            assertEquals(context.getExpirationTimeMillis(), task.getStartTime() + keepAlive.millis());

            // search success or failure
            asyncSearchState = randomBoolean() ?
                    stateMachine.trigger(new SearchSuccessfulEvent(context, getMockSearchResponse())) :
                    stateMachine.trigger(new SearchFailureEvent(context, new RuntimeException("test")));
            assertTrue(asyncSearchState.equals(SUCCEEDED) || asyncSearchState.equals(FAILED));
            if (asyncSearchState.equals(SUCCEEDED)) {
                assertNotNull(context.getSearchResponse());
                assertNull(context.getSearchError());

            } else {
                assertNotNull(context.getSearchError());
                assertNull(context.getSearchResponse());
            }
            if (keepOnCompletion) {
                //persisted or persist failed
                asyncSearchState = randomBoolean() ? stateMachine.trigger(new SearchResponsePersistedEvent(context)) :
                        stateMachine.trigger(new SearchResponsePersistFailedEvent(context));
                assertTrue(asyncSearchState.equals(PERSISTED) || asyncSearchState.equals(PERSIST_FAILED));

            } else {
                stateMachine.trigger(new SearchDeletionEvent(context));
                assertEquals(context.getAsyncSearchState(), DELETED);
            }

        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testDeletionOfRunningAsyncSearch() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            String node = UUID.randomUUID().toString();
            AsyncSearchStateMachine stateMachine = getInstanceFromNode(AsyncSearchStateMachine.class);
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(), threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = true;
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            SearchTask task = new SearchTask(randomNonNegativeLong(), "transport", SearchAction.NAME,
                    null, null, Collections.emptyMap());
            AsyncSearchState asyncSearchState = stateMachine.trigger(new SearchStartedEvent(context,
                    task));
            assertEquals(asyncSearchState, RUNNING);
            assertTrue(context.shouldPersist());

            asyncSearchState = stateMachine.trigger(new SearchDeletionEvent(context));
            assertFalse(context.shouldPersist());
            assertEquals(context.getAsyncSearchState(), DELETED);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testConcurrentUpdatesToAsyncSearchStage() throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
            AsyncSearchStateMachine stateMachine = getInstanceFromNode(AsyncSearchStateMachine.class);
            String node = UUID.randomUUID().toString();
            AsyncSearchProgressListener asyncSearchProgressListener = new AsyncSearchProgressListener(
                    threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(), threadPool::relativeTimeInMillis);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            assertEquals(context.getAsyncSearchState(), INIT);
            doConcurrentStageAdvancement(stateMachine, new SearchStartedEvent(context, new SearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, null, null, Collections.emptyMap())), RUNNING, AsyncSearchStateMachineException.class);
            if (randomBoolean()) {//success
                doConcurrentStageAdvancement(stateMachine, new SearchSuccessfulEvent(context, getMockSearchResponse()), SUCCEEDED,
                        AsyncSearchStateMachineException.class);
            } else {
                doConcurrentStageAdvancement(stateMachine, new SearchFailureEvent(context, new RuntimeException("test")), FAILED,
                        AsyncSearchStateMachineException.class);
            }
            if (randomBoolean()) { //deletion
                doConcurrentStageAdvancement(stateMachine, new SearchDeletionEvent(context), DELETED,
                        AsyncSearchStateMachineException.class);
            } else {
                if (randomBoolean()) { //persistence succeeded
                    doConcurrentStageAdvancement(stateMachine, new SearchResponsePersistedEvent(context), PERSISTED,
                            AsyncSearchStateMachineException.class);
                } else {
                    doConcurrentStageAdvancement(stateMachine, new SearchResponsePersistedEvent(context), PERSIST_FAILED,
                            AsyncSearchStateMachineException.class);
                }
            }

        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    private <T extends Throwable> void doConcurrentStageAdvancement(AsyncSearchStateMachine asyncSearchStateMachine,
                                                                    AsyncSearchContextEvent event,
                                                                    AsyncSearchState finalState,
                                                                    Class<T> throwable) throws InterruptedException {
        int numThreads = 10;
        List<Thread> operationThreads = new ArrayList<>();
        AtomicInteger numUpdateSuccesses = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    asyncSearchStateMachine.trigger(event);
                    numUpdateSuccesses.getAndIncrement();
                } catch (Exception e) {
                    assertTrue(throwable.isInstance(e));
                }
            });
            operationThreads.add(thread);
        }
        operationThreads.forEach(Thread::start);
        for (Thread operationThread : operationThreads) {
            operationThread.join();
        }
        assertEquals(1, numUpdateSuccesses.get());
        assertEquals(event.asyncSearchContext().getAsyncSearchState(), finalState);
    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

