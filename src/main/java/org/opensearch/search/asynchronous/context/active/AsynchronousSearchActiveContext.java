/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.active;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.permits.AsynchronousSearchContextPermits;
import org.opensearch.search.asynchronous.context.permits.NoopAsynchronousSearchContextPermits;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.common.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchProgressActionListener;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static org.opensearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask}
 * and {@link SearchProgressActionListener}
 */
public class AsynchronousSearchActiveContext extends AsynchronousSearchContext implements Closeable {

    private final SetOnce<SearchTask> searchTask;
    private volatile long expirationTimeMillis;
    private long startTimeMillis;
    private final Boolean keepOnCompletion;
    private final TimeValue keepAlive;
    private final String nodeId;
    private final SetOnce<String> asynchronousSearchId;
    private final AtomicBoolean completed;
    private final SetOnce<Exception> error;
    private final SetOnce<SearchResponse> searchResponse;
    private final AtomicBoolean closed;
    private final Supplier<Boolean> persistSearchFailureSupplier;
    private final AsynchronousSearchContextPermits asynchronousSearchContextPermits;
    private final Supplier<SearchResponse> partialResponseSupplier;
    @Nullable
    private final User user;

    public AsynchronousSearchActiveContext(AsynchronousSearchContextId asynchronousSearchContextId, String nodeId,
                                           TimeValue keepAlive, boolean keepOnCompletion, ThreadPool threadPool,
                                           LongSupplier currentTimeSupplier,
                                           AsynchronousSearchProgressListener asynchronousSearchProgressListener, @Nullable User user,
                                           Supplier<Boolean> persistSearchFailureSupplier) {
        super(asynchronousSearchContextId, currentTimeSupplier);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new SetOnce<>();
        this.searchResponse = new SetOnce<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.asynchronousSearchProgressListener = asynchronousSearchProgressListener;
        this.partialResponseSupplier = () -> asynchronousSearchProgressListener.partialResponse();
        this.searchTask = new SetOnce<>();
        this.asynchronousSearchId = new SetOnce<>();
        this.completed = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.asynchronousSearchContextPermits = keepOnCompletion ? new AsynchronousSearchContextPermits(asynchronousSearchContextId,
                threadPool) : new NoopAsynchronousSearchContextPermits(asynchronousSearchContextId);
        this.user = user;
        this.persistSearchFailureSupplier = persistSearchFailureSupplier;
    }

    public void setTask(SearchTask searchTask) {
        assert isAlive();
        assert currentStage == INIT;
        Objects.requireNonNull(searchTask);
        searchTask.setProgressListener(asynchronousSearchProgressListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.asynchronousSearchId.set(AsynchronousSearchIdConverter.buildAsyncId(new AsynchronousSearchId(nodeId, searchTask.getId(),
                getContextId())));
    }

    public void processSearchFailure(Exception e) {
        assert isAlive();
        // we don't want to process stack traces
        try {
            if (e.getCause() != null) {
                e.getCause().setStackTrace(new StackTraceElement[0]);
            }
            this.error.set(e);
        } finally {
            boolean result = completed.compareAndSet(false, true);
            assert result : "Process search failure already complete";
        }
    }

    public void processSearchResponse(SearchResponse response) {
        assert isAlive();
        try {
            ShardSearchFailure[] shardSearchFailures = response.getShardFailures();
            for (ShardSearchFailure shardSearchFailure : shardSearchFailures) {
                // we don't want to process stack traces
                if (shardSearchFailure.getCause() != null) {
                    shardSearchFailure.getCause().setStackTrace(new StackTraceElement[0]);
                }
            }
            this.searchResponse.set(response);
        } finally {
            boolean result = completed.compareAndSet(false, true);
            assert result : "Process search response already complete";
        }
    }

    @Override
    public SearchResponse getSearchResponse() {
        return completed.get() ? searchResponse.get() : partialResponseSupplier.get();
    }

    @Override
    public String getAsynchronousSearchId() {
        return asynchronousSearchId.get();
    }

    public boolean shouldPersist() {
        return keepOnCompletion && isExpired() == false && isAlive() && (error.get() == null || persistSearchFailureSupplier.get());
    }

    public boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public void setExpirationTimeMillis(long expirationTimeMillis) {
        assert isAlive();
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    @Override
    public Exception getSearchError() {
        return error.get();
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    @Override
    public User getUser() {
        return user;
    }

    public void acquireContextPermitIfRequired(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asynchronousSearchContextPermits.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asynchronousSearchContextPermits.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }

    public boolean isAlive() {
        if (closed.get()) {
            assert getAsynchronousSearchState() == CLOSED : "State must be closed for asynchronous search id " + getAsynchronousSearchId();
            return false;
        }
        return true;
    }

    public boolean isCompleted() {
        return completed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            asynchronousSearchContextPermits.close();
        }
    }
}
