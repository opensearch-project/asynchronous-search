/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
