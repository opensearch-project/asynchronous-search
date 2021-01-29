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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.active;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits.AsynchronousSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits.NoopAsynchronousSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;

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
    private AsynchronousSearchContextPermits asynchronousSearchContextPermits;
    @Nullable
    private final User user;

    public AsynchronousSearchActiveContext(AsynchronousSearchContextId asynchronousSearchContextId, String nodeId,
                                    TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                    AsynchronousSearchProgressListener searchProgressActionListener,
                                    @Nullable User user) {
        super(asynchronousSearchContextId, currentTimeSupplier);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new SetOnce<>();
        this.searchResponse = new SetOnce<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.asynchronousSearchProgressListener = searchProgressActionListener;
        this.searchTask = new SetOnce<>();
        this.asynchronousSearchId = new SetOnce<>();
        this.completed = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.asynchronousSearchContextPermits = keepOnCompletion ? new AsynchronousSearchContextPermits(asynchronousSearchContextId,
                threadPool) :
                new NoopAsynchronousSearchContextPermits(asynchronousSearchContextId);
        this.user = user;
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
        if (completed.compareAndSet(false, true)) {
            // we don't want to process stack traces
            if (e.getCause() != null) {
                e.getCause().setStackTrace(new StackTraceElement[0]);
            }
            error.set(e);
        }
    }

    public void processSearchResponse(SearchResponse response) {
        assert isAlive();
        if (completed.compareAndSet(false, true)) {
            ShardSearchFailure [] shardSearchFailures = response.getShardFailures();
            for(ShardSearchFailure shardSearchFailure : shardSearchFailures) {
                // we don't want to process stack traces
                if (shardSearchFailure.getCause() != null) {
                    shardSearchFailure.getCause().setStackTrace(new StackTraceElement[0]);
                }
            }
            this.searchResponse.set(response);
        }
    }

    @Override
    public SearchResponse getSearchResponse() {
        if (searchResponse.get() != null) {
            return searchResponse.get();
        } else {
            return asynchronousSearchProgressListener.partialResponse();
        }
    }

    @Override
    public String getAsynchronousSearchId() {
        return asynchronousSearchId.get();
    }

    public boolean shouldPersist() {
        return keepOnCompletion && isExpired() == false && isAlive();
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
