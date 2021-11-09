/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.active;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;

public class AsynchronousSearchContextClosedException extends Exception {

    private final AsynchronousSearchContextId asynchronousSearchContextId;

    public AsynchronousSearchContextClosedException(AsynchronousSearchContextId asynchronousSearchContextId) {
        super("message");
        this.asynchronousSearchContextId = asynchronousSearchContextId;
    }

    public AsynchronousSearchContextId getAsynchronousSearchContextId() {
        return asynchronousSearchContextId;
    }
}
