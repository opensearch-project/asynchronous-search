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

package com.amazon.opendistroforelasticsearch.search.async.context.state;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;

import java.util.function.BiConsumer;

public class AsyncSearchTransition<Event extends AsyncSearchContextEvent> implements Transition<AsyncSearchState, Event> {

    private final AsyncSearchState sourceState;
    private final AsyncSearchState targetState;
    private final BiConsumer<AsyncSearchState, Event> onEvent;
    private final BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener;
    private final Class<Event> eventType;

    public AsyncSearchTransition(AsyncSearchState sourceState, AsyncSearchState targetState,
                                 BiConsumer<AsyncSearchState, Event> onEvent,
                                 BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener,
                                 Class<Event> eventName) {
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.onEvent = onEvent;
        this.eventListener = eventListener;
        this.eventType = eventName;
    }

    @Override
    public AsyncSearchState sourceState() {
        return sourceState;
    }

    @Override
    public AsyncSearchState targetState() {
        return targetState;
    }

    @Override
    public Class<? extends Event> eventType() {
        return eventType;
    }

    @Override
    public BiConsumer<AsyncSearchState, Event> onEvent() {
        return onEvent;
    }

    @Override
    public BiConsumer<AsyncSearchContextId, AsyncSearchContextListener> eventListener() {
        return eventListener;
    }
}
