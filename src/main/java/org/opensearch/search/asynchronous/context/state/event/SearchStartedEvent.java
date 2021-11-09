/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchTask;

/**
 * Event triggered when
 * {@linkplain org.opensearch.action.search.TransportSearchAction#execute(ActionRequest, ActionListener)} is fired, to
 * signal the search has begun.
 */
public class SearchStartedEvent extends AsynchronousSearchContextEvent {

    private final SearchTask searchTask;

    public SearchStartedEvent(AsynchronousSearchContext asynchronousSearchContext, SearchTask searchTask) {
        super(asynchronousSearchContext);
        this.searchTask = searchTask;
    }

    @Override
    public AsynchronousSearchContext asynchronousSearchContext() {
        return asynchronousSearchContext;
    }

    public SearchTask getSearchTask() {
        return searchTask;
    }
}
