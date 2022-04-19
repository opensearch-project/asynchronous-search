/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;

/**
 * Event triggered when asynchronous search response is successfully stored in system index.
 */
public class SearchResponsePersistedEvent extends AsynchronousSearchContextEvent {

    public SearchResponsePersistedEvent(AsynchronousSearchContext asynchronousSearchContext) {
        super(asynchronousSearchContext);
    }
}

