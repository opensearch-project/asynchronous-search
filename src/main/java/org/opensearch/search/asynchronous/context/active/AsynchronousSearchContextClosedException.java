/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
