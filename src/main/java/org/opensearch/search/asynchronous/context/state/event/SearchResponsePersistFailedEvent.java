/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;

/**
 * Event to trigger when failure occurs while storing asynchronous search response in system index.
 */
public class SearchResponsePersistFailedEvent extends AsynchronousSearchContextEvent {

    public SearchResponsePersistFailedEvent(AsynchronousSearchContext asynchronousSearchContext) {
        super(asynchronousSearchContext);
    }
}
