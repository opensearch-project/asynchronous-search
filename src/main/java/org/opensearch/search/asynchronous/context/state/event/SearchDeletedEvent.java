/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;

public class SearchDeletedEvent extends AsynchronousSearchContextEvent {

    public SearchDeletedEvent(AsynchronousSearchActiveContext asynchronousSearchActiveContext) {
        super(asynchronousSearchActiveContext);
    }
}
