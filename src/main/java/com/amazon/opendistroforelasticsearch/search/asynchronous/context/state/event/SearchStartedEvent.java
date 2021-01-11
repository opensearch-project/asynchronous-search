/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchTask;

/**
 * Event triggered when
 * {@linkplain org.elasticsearch.action.search.TransportSearchAction#execute(ActionRequest, ActionListener)} is fired, to
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
