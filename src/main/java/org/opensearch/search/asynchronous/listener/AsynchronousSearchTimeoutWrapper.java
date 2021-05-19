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

package org.opensearch.search.asynchronous.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/***
 * The wrapper that schedules a timeout and wraps it in an actual {@link ActionListener}. The {@link AsynchronousSearchTimeoutWrapper}
 * guarantees that only one of timeout/response/failure gets executed so that responses are not sent over a closed channel
 * The wrap and actual scheduling has been split to avoid races in listeners as they get attached.
 */
public class AsynchronousSearchTimeoutWrapper {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchTimeoutWrapper.class);

    /***
     * Wraps the listener and schedules the timeout
     * @param threadPool threadPool
     * @param timeout timeout
     * @param executor executor
     * @param actionListener actionListener
     * @param timeoutConsumer timeoutConsumer
     * @param <Response> the response
     * @return PrioritizedListener
     */
    public static <Response> PrioritizedActionListener<Response> wrapScheduledTimeout(ThreadPool threadPool, TimeValue timeout,
                                                                                      String executor,
                                                                                      ActionListener<Response> actionListener,
                                                                                      Consumer<ActionListener<Response>> timeoutConsumer) {
        return scheduleTimeout(threadPool, timeout, executor, initListener(actionListener, timeoutConsumer));
    }

    /**
     *
     * @param actionListener actionListener
     * @param timeoutConsumer timeoutConsumer
     * @param <Response> Response
     * @return PrioritizedListener
     */
    public static <Response> PrioritizedActionListener<Response> initListener(ActionListener<Response> actionListener,
                                                                              Consumer<ActionListener<Response>> timeoutConsumer) {
        CompletionPrioritizedActionListener<Response> completionTimeoutListener =
                new CompletionPrioritizedActionListener<>(actionListener, timeoutConsumer);
        return completionTimeoutListener;
    }

    /**
     *
     * @param threadPool the thread pool instance
     * @param timeout the configured timeout
     * @param executor the thread pool name
     * @param completionTimeoutListener the listener
     * @param <Response> Response
     * @return PrioritizedListener
     */
    public static <Response> PrioritizedActionListener<Response> scheduleTimeout(ThreadPool threadPool, TimeValue timeout, String executor,
                                                                       PrioritizedActionListener<Response> completionTimeoutListener) {
        ((CompletionPrioritizedActionListener)completionTimeoutListener).cancellable = threadPool.schedule(
                (Runnable) completionTimeoutListener, timeout, executor);
        return completionTimeoutListener;
    }

    static class CompletionPrioritizedActionListener<Response> implements PrioritizedActionListener<Response>, Runnable {
        private final ActionListener<Response> actionListener;
        private volatile Scheduler.ScheduledCancellable cancellable;
        private final AtomicBoolean complete = new AtomicBoolean(false);
        private final Consumer<ActionListener<Response>> timeoutConsumer;

        CompletionPrioritizedActionListener(ActionListener<Response> actionListener, Consumer<ActionListener<Response>> timeoutConsumer) {
            this.actionListener = actionListener;
            this.timeoutConsumer = timeoutConsumer;
        }

        void cancel() {
            if (cancellable != null && cancellable.isCancelled() == false) {
                cancellable.cancel();
            }
        }

        @Override
        public void run() {
            executeImmediately();
        }

        @Override
        public void executeImmediately() {
            if (complete.compareAndSet(false, true)) {
                cancel();
                timeoutConsumer.accept(this);
            }
        }

        @Override
        public void onResponse(Response response) {
            if (complete.compareAndSet(false, true)) {
                cancel();
                actionListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (complete.compareAndSet(false, true)) {
                cancel();
                actionListener.onFailure(e);
            }
        }
    }
}
