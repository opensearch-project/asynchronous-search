/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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
