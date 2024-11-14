/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
