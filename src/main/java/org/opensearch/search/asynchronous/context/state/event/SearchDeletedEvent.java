/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;

public class SearchDeletedEvent extends AsynchronousSearchContextEvent {

    public SearchDeletedEvent(AsynchronousSearchActiveContext asynchronousSearchActiveContext) {
        super(asynchronousSearchActiveContext);
    }
}
