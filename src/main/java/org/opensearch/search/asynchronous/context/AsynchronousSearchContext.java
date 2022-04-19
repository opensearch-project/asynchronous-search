/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;

import java.util.function.LongSupplier;


/**
 * Wrapper around information that needs to stay around when an asynchronous search has been submitted.
 * This class encapsulates the details of the various elements pertaining to an asynchronous search, including the
 * {@linkplain AsynchronousSearchId}, the start time, the updatable expiration time, the search response - completed or partial, the
 * error(if the underlying search request fails), the {@linkplain AsynchronousSearchProgressListener} and the current
 * {@linkplain AsynchronousSearchState} that the asynchronous search execution has reached in its lifecycle.
 */
public abstract class AsynchronousSearchContext {

    protected final AsynchronousSearchContextId asynchronousSearchContextId;
    protected final LongSupplier currentTimeSupplier;
    protected volatile AsynchronousSearchState currentStage = AsynchronousSearchState.INIT;
    protected volatile AsynchronousSearchProgressListener asynchronousSearchProgressListener;

    public AsynchronousSearchContext(AsynchronousSearchContextId asynchronousSearchContextId, LongSupplier currentTimeSupplier) {
        this.asynchronousSearchContextId = asynchronousSearchContextId;
        this.currentTimeSupplier = currentTimeSupplier;
    }

    public @Nullable
    AsynchronousSearchProgressListener getAsynchronousSearchProgressListener() {
        return asynchronousSearchProgressListener;
    }

    public AsynchronousSearchState getAsynchronousSearchState() {
        return currentStage;
    }

    public boolean isRunning() {
        return getAsynchronousSearchState() == AsynchronousSearchState.RUNNING;
    }

    public AsynchronousSearchContextId getContextId() {
        return asynchronousSearchContextId;
    }

    public abstract String getAsynchronousSearchId();

    public abstract long getExpirationTimeMillis();

    public abstract long getStartTimeMillis();

    public abstract @Nullable
    SearchResponse getSearchResponse();

    public abstract @Nullable
    Exception getSearchError();

    public abstract @Nullable
    User getUser();

    public boolean isExpired() {
        return getExpirationTimeMillis() < currentTimeSupplier.getAsLong();
    }

    public AsynchronousSearchResponse getAsynchronousSearchResponse() {
        return new AsynchronousSearchResponse(getAsynchronousSearchId(), getAsynchronousSearchState(), getStartTimeMillis(),
                getExpirationTimeMillis(), getSearchResponse(), getSearchError());
    }

    public void setState(AsynchronousSearchState targetState) {
        this.currentStage = targetState;
    }
}
