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

package org.opensearch.search.asynchronous.context;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
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
