/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state;

import java.util.Locale;

public class AsynchronousSearchStateMachineClosedException extends Exception {

    private final AsynchronousSearchState currentState;
    private final AsynchronousSearchContextEvent contextEvent;

    public AsynchronousSearchStateMachineClosedException(
        AsynchronousSearchState currentState,
        AsynchronousSearchContextEvent contextEvent
    ) {
        super(
            String.format(
                Locale.ROOT,
                "Invalid transition for CLOSED context [%s] from source state [%s] on event [%s]",
                contextEvent.asynchronousSearchContext.getAsynchronousSearchId(),
                currentState,
                contextEvent.getClass().getName()
            )
        );
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
