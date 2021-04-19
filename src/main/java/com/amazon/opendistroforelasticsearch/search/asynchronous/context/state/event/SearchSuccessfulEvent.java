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
