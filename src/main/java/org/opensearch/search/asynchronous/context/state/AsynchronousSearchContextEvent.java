/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;

import java.util.Objects;

/**
 * The AsynchronousSearchContextEvent on which the transitions take place
 */
public abstract class AsynchronousSearchContextEvent {

    protected final AsynchronousSearchContext asynchronousSearchContext;

    protected AsynchronousSearchContextEvent(AsynchronousSearchContext asynchronousSearchContext) {
        Objects.requireNonNull(asynchronousSearchContext);
        this.asynchronousSearchContext = asynchronousSearchContext;
    }

    public AsynchronousSearchContext asynchronousSearchContext() {
        return asynchronousSearchContext;
    }

}

