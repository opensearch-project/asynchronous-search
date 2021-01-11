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

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.permits.AsyncSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.async.context.permits.NoopAsyncSearchContextPermits;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
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

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask}
 * and {@link SearchProgressActionListener}
 */
public class AsyncSearchActiveContext extends AsyncSearchContext implements Closeable {

    private final SetOnce<SearchTask> searchTask;
    private volatile long expirationTimeMillis;
    private long startTimeMillis;
    private final Boolean keepOnCompletion;
    private final TimeValue keepAlive;
    private final String nodeId;
    private final SetOnce<String> asyncSearchId;
    private final AtomicBoolean completed;
    private final SetOnce<Exception> error;
    private final SetOnce<SearchResponse> searchResponse;
    private final AtomicBoolean closed;
    private AsyncSearchContextPermits asyncSearchContextPermits;
    @Nullable
    private final User user;

    public AsyncSearchActiveContext(AsyncSearchContextId asyncSearchContextId, String nodeId,
                                    TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, LongSupplier currentTimeSupplier,
                                    AsyncSearchProgressListener searchProgressActionListener,
                                    @Nullable User user) {
        super(asyncSearchContextId, currentTimeSupplier);
        this.keepOnCompletion = keepOnCompletion;
        this.error = new SetOnce<>();
        this.searchResponse = new SetOnce<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.asyncSearchProgressListener = searchProgressActionListener;
        this.searchTask = new SetOnce<>();
        this.asyncSearchId = new SetOnce<>();
        this.completed = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.asyncSearchContextPermits = keepOnCompletion ? new AsyncSearchContextPermits(asyncSearchContextId, threadPool) :
                new NoopAsyncSearchContextPermits(asyncSearchContextId);
        this.user = user;
    }

    public void setTask(SearchTask searchTask) {
        assert isAlive();
        assert currentStage == INIT;
        Objects.requireNonNull(searchTask);
        searchTask.setProgressListener(asyncSearchProgressListener);
        this.searchTask.set(searchTask);
        this.startTimeMillis = searchTask.getStartTime();
        this.expirationTimeMillis = startTimeMillis + keepAlive.getMillis();
        this.asyncSearchId.set(AsyncSearchIdConverter.buildAsyncId(new AsyncSearchId(nodeId, searchTask.getId(), getContextId())));
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
            return asyncSearchProgressListener.partialResponse();
        }
    }

    @Override
    public String getAsyncSearchId() {
        return asyncSearchId.get();
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
        asyncSearchContextPermits.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermits(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermits.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }

    public boolean isAlive() {
        if (closed.get()) {
            assert getAsyncSearchState() == CLOSED : "State must be closed for async search id " + getAsyncSearchId();
            return false;
        }
        return true;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            asyncSearchContextPermits.close();
        }
    }
}
