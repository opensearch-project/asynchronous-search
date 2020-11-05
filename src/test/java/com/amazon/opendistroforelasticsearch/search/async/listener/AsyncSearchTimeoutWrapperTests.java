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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AsyncSearchTimeoutWrapperTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "random node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testTimeoutConsumerInvokedOnTimeout() {
        TimeValue timeout = TimeValue.timeValueMillis(randomNonNegativeLong());
        AtomicBoolean onResponseInvoked = new AtomicBoolean(false);
        AtomicBoolean onTimeoutInvoked = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        PrioritizedActionListener<Void> listener = mockListener(onResponseInvoked, exception);
        PrioritizedActionListener<Void> prioritizedActionListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(
                deterministicTaskQueue.getThreadPool(), timeout, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                listener, (r) -> assertTrue(onTimeoutInvoked.compareAndSet(false, true)));
        //simulate timeout by advancing time
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        //Fire onResponse/onFailure from search action listener
        if (randomBoolean()) {
            prioritizedActionListener.onResponse(null);
        } else {
            prioritizedActionListener.onFailure(new RuntimeException("random exception"));
        }
        //assert only the timeout consumer gets executed
        assertTrue(onTimeoutInvoked.get());
        assertFalse(onResponseInvoked.get());
        assertNull(exception.get());
    }

    public void testResponseBeforeTimeout() {
        TimeValue timeout = TimeValue.timeValueMillis(randomNonNegativeLong());
        AtomicBoolean onResponseInvoked = new AtomicBoolean(false);
        AtomicBoolean onTimeoutInvoked = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = mockListener(onResponseInvoked, exception);
        PrioritizedActionListener<Void> prioritizedActionListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(
                deterministicTaskQueue.getThreadPool(), timeout, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                listener, (r) -> assertTrue(onTimeoutInvoked.compareAndSet(false, true)));

        //Fire on Response of the action listener
        prioritizedActionListener.onResponse(null);

        //simulate timeout by advancing time
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertFalse(onTimeoutInvoked.get());
        assertTrue(onResponseInvoked.get());
        assertNull(exception.get());
    }

    public void testExceptionBeforeTimeout() {
        TimeValue timeout = TimeValue.timeValueMillis(randomNonNegativeLong());
        AtomicBoolean onResponseInvoked = new AtomicBoolean(false);
        AtomicBoolean onTimeoutInvoked = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = mockListener(onResponseInvoked, exception);
        PrioritizedActionListener<Void> prioritizedActionListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(
                deterministicTaskQueue.getThreadPool(), timeout, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                listener, (r) -> assertTrue(onTimeoutInvoked.compareAndSet(false, true)));

        prioritizedActionListener.onFailure(new RuntimeException("random exception"));

        //simulate timeout by advancing time
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();

        assertFalse(onTimeoutInvoked.get());
        assertFalse(onResponseInvoked.get());
        assertThat(exception.get(), instanceOf(RuntimeException.class));
    }

    public void testExecuteImmediately() {
        TimeValue timeout = TimeValue.timeValueMillis(randomLongBetween(100, 500));
        AtomicBoolean onResponseInvoked = new AtomicBoolean(false);
        AtomicBoolean onTimeoutInvoked = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = mockListener(onResponseInvoked, exception);
        PrioritizedActionListener<Void> prioritizedActionListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(
                deterministicTaskQueue.getThreadPool(), timeout, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                listener, (r) -> assertTrue(onTimeoutInvoked.compareAndSet(false, true)));

        //execute the listener immediately
        prioritizedActionListener.executeImmediately();

        assertTrue(onTimeoutInvoked.get());
        assertFalse(onResponseInvoked.get());
        assertNull(exception.get());

        //simulate timeout by advancing time
        assertTrue(deterministicTaskQueue.hasDeferredTasks());
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
    }

    private PrioritizedActionListener<Void> mockListener(AtomicBoolean response, AtomicReference<Exception> exception) {
        return new PrioritizedActionListener<Void>() {

            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void executeImmediately() {
                assertTrue(completed.compareAndSet(false, true));
            }

            @Override
            public void onResponse(Void aVoid) {
                assertTrue(completed.compareAndSet(false, true));
                assertTrue(response.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(completed.compareAndSet(false, true));
                assertTrue(exception.compareAndSet(null, e));
            }
        };
    }
}
