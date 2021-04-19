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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchContextClosedException;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.RunOnce;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/***
 * The permit needed by any mutating operation on {@link AsynchronousSearchContext} while it is being moved over to the
 * persistence store. Each mutating operation acquires a single permit while the AsynchronousSearchPostProcessor acquires all permits
 * before it transitions context to the index. Provides fairness to consumers and throws {@linkplain TimeoutException} after
 * maximum time has elapsed waiting for the in-flight operations block.
 */
public class AsynchronousSearchContextPermits implements Closeable {

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;

    final Semaphore semaphore;
    protected final AsynchronousSearchContextId asynchronousSearchContextId;
    private volatile String lockDetails;
    private final ThreadPool threadPool;
    protected volatile boolean closed;

    protected static final Logger logger = LogManager.getLogger(AsynchronousSearchContextPermits.class);

    public AsynchronousSearchContextPermits(AsynchronousSearchContextId asynchronousSearchContextId, ThreadPool threadPool) {
        this.asynchronousSearchContextId = asynchronousSearchContextId;
        this.threadPool = threadPool;
        this.semaphore = new Semaphore(TOTAL_PERMITS, true);
    }

    public AsynchronousSearchContextPermits(AsynchronousSearchContextId asynchronousSearchContextId, ThreadPool threadPool,
                                            Semaphore semaphore) {
        this.asynchronousSearchContextId = asynchronousSearchContextId;
        this.threadPool = threadPool;
        this.semaphore = semaphore;
    }

    private Releasable acquirePermits(int permits, TimeValue timeout, final String details) throws AsynchronousSearchContextClosedException,
            TimeoutException {
        RunOnce release = new RunOnce(() -> {});
        if (closed) {
            logger.debug("Trying to acquire permit for closed context [{}]", asynchronousSearchContextId);
            throw new AsynchronousSearchContextClosedException(asynchronousSearchContextId);
        }
        try {
            if (semaphore.tryAcquire(permits, timeout.getMillis(), TimeUnit.MILLISECONDS)) {
                this.lockDetails = details;
                release = new RunOnce(() -> {
                    logger.debug("Releasing permit(s) [{}] with reason [{}]", permits, lockDetails);
                    semaphore.release(permits);});
                if (closed) {
                    release.run();
                    logger.debug("Trying to acquire permit for closed context [{}]", asynchronousSearchContextId);
                    throw new AsynchronousSearchContextClosedException( asynchronousSearchContextId);
                }
                return release::run;
            } else {
                throw new TimeoutException("obtaining context lock" + asynchronousSearchContextId + "timed out after " +
                        timeout.getMillis() + "ms, previous lock details: [" + lockDetails + "] trying to lock for [" + details + "]");
            }
        } catch (InterruptedException e ) {
            Thread.currentThread().interrupt();
            release.run();
            throw new RuntimeException("thread interrupted while trying to obtain context lock", e);
        }
    }

    private void asyncAcquirePermit(int permits, final ActionListener<Releasable> onAcquired, final TimeValue timeout, String reason) {
        threadPool.executor(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME).execute(new AbstractRunnable() {
            @Override
            public void onFailure(final Exception e) {
                logger.debug(() -> new ParameterizedMessage("Failed to acquire permit [{}] for [{}]",
                        permits, reason), e);
                onAcquired.onFailure(e);
            }

            @Override
            protected void doRun() throws TimeoutException, AsynchronousSearchContextClosedException {
                final Releasable releasable = acquirePermits(permits, timeout, reason);
                logger.debug("Successfully acquired context permit [{}] for [{}]", permits, reason);
                onAcquired.onResponse(releasable);
            }
        });
    }

    /***
     * Acquire the permit in an asynchronous fashion so as to not block the thread while acquiring.
     * The {@link ActionListener} is invoked if the mutex was successfully acquired within the timeout. The caller has a
     * responsibility of executing the {@link Releasable}
     * on completion or failure of the operation run within the permit
     *
     * @param onAcquired the releasable that must be invoked
     * @param timeout the timeout within which the permit must be acquired or deemed failed
     * @param reason the reason for acquiring the permit
     */
    public void asyncAcquirePermit(final ActionListener<Releasable> onAcquired, final TimeValue timeout, String reason) {
        asyncAcquirePermit(1, onAcquired, timeout, reason);
    }

    /***
     * Acquire all the permits in an asynchronous fashion so as to not block the thread while acquiring.
     * The {@link ActionListener} is invoked if the mutex was successfully acquired within the timeout. The caller has a
     * responsibility of executing the {@link Releasable}
     * on completion or failure of the operation run within the permit
     *
     * @param onAcquired the releasable that must be invoked
     * @param timeout the timeout within which the permit must be acquired or deemed failed
     * @param reason the reason for acquiring the permit
     */
    public void asyncAcquireAllPermits(final ActionListener<Releasable> onAcquired, final TimeValue timeout, String reason) {
        asyncAcquirePermit(TOTAL_PERMITS, onAcquired, timeout, reason);
    }

    @Override
    public void close() {
        closed = true;
    }
}
