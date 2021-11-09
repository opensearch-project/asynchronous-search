/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state;

import org.opensearch.action.search.SearchTask;

/**
 * The state of the asynchronous search.
 */
public enum AsynchronousSearchState {

    /**
     * At the start of the search, before the {@link SearchTask} starts to run
     */
    INIT,

    /**
     * The search state actually has been started
     */
    RUNNING,

    /**
     * The search has completed successfully
     */
    SUCCEEDED,

    /**
     * The search execution has failed
     */
    FAILED,

    /**
     * The response is starting to get persisted
     */
    PERSISTING,

    /**
     * The context has been persisted to system index
     */
    PERSIST_SUCCEEDED,

    /**
     * The context has failed to persist to system index
     */
    PERSIST_FAILED,

    /**
     * The context has been deleted
     */
    CLOSED,

    /**
     * The context has been retrieved from asynchronous search response system index
     */
    STORE_RESIDENT
}
