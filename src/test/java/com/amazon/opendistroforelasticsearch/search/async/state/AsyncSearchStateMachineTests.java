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

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchTransition;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.DELETED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;

public class AsyncSearchStateMachineTests extends ESTestCase {

    private final String node = UUID.randomUUID().toString();

    public void testMachine() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool("test");
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

            AtomicBoolean searchStarted = new AtomicBoolean();
            AtomicBoolean searchSuccessful = new AtomicBoolean();
            AsyncSearchStateMachine stateMachine = new AsyncSearchStateMachine(
                    EnumSet.allOf(AsyncSearchState.class), INIT);
            stateMachine.markTerminalStates(EnumSet.of(DELETED, PERSIST_FAILED, PERSISTED));

            stateMachine.registerTransition(new AsyncSearchTransition<>(INIT, RUNNING,
                    (s, e) -> assertTrue(searchStarted.compareAndSet(false, true)),
                    (contextId, listener) -> listener.onContextRunning(contextId), SearchStartedEvent.class));
            stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, SUCCEEDED,
                    (s, e) -> assertTrue(searchSuccessful.compareAndSet(false, true)),
                    (contextId, listener) -> listener.onContextCompleted(contextId), SearchSuccessfulEvent.class));

            stateMachine.trigger(new SearchStartedEvent(context, null));
            assertTrue(searchStarted.get());
            assertFalse(searchSuccessful.get());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
}
