/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;

/**
 * Event to trigger when an asynchronous search completes with an error.
 */
public class SearchFailureEvent extends AsynchronousSearchContextEvent {

    private final Exception exception;

    public SearchFailureEvent(AsynchronousSearchContext asynchronousSearchContext, Exception exception) {
        super(asynchronousSearchContext);
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
