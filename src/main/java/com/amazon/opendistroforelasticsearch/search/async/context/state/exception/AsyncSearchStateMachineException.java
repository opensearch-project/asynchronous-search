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

package com.amazon.opendistroforelasticsearch.search.async.context.state.exception;

import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;

public class AsyncSearchStateMachineException extends IllegalStateException {
    private final AsyncSearchState currentState;
    private final AsyncSearchContextEvent event;

    public AsyncSearchStateMachineException(AsyncSearchState currentState, AsyncSearchContextEvent event) {
        super(event.asyncSearchContext().getAsyncSearchId()
                + " cannot transition from [" + currentState + "] on event " + event.getClass());
        this.event = event;
        this.currentState = currentState;
    }
}
