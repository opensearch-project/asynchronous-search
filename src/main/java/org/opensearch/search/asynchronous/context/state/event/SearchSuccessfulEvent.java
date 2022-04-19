/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import org.opensearch.action.search.SearchResponse;

/**
 * Event triggered when asynchronous search completes with a successful search response.
 */
public class SearchSuccessfulEvent extends AsynchronousSearchContextEvent {

    private SearchResponse searchResponse;

    public SearchSuccessfulEvent(AsynchronousSearchContext asynchronousSearchContext, SearchResponse searchResponse) {
        super(asynchronousSearchContext);
        this.searchResponse = searchResponse;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
