/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state;

import java.util.Locale;

public class AsynchronousSearchStateMachineClosedException extends Exception {

    private final AsynchronousSearchState currentState;
    private final AsynchronousSearchContextEvent contextEvent;

    public AsynchronousSearchStateMachineClosedException(AsynchronousSearchState currentState,
                                                         AsynchronousSearchContextEvent contextEvent) {
        super(String.format(Locale.ROOT, "Invalid transition for CLOSED context [%s] from source state [%s] on event [%s]",
                contextEvent.asynchronousSearchContext.getAsynchronousSearchId(), currentState, contextEvent.getClass().getName()));
        this.currentState = currentState;
        this.contextEvent = contextEvent;
    }

    public AsynchronousSearchState getCurrentState() {
        return currentState;
    }

    public AsynchronousSearchContextEvent getContextEvent() {
        return contextEvent;
    }
}
