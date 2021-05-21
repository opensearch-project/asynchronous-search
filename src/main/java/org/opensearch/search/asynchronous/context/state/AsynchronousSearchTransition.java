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

package org.opensearch.search.asynchronous.context.state;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;

import java.util.function.BiConsumer;

public class AsynchronousSearchTransition<Event extends AsynchronousSearchContextEvent>
        implements Transition<AsynchronousSearchState, Event> {

    private final AsynchronousSearchState sourceState;
    private final AsynchronousSearchState targetState;
    private final BiConsumer<AsynchronousSearchState, Event> onEvent;
    private final BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener;
    private final Class<Event> eventType;

    public AsynchronousSearchTransition(AsynchronousSearchState sourceState, AsynchronousSearchState targetState,
                                 BiConsumer<AsynchronousSearchState, Event> onEvent,
                                 BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener,
                                 Class<Event> eventName) {
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.onEvent = onEvent;
        this.eventListener = eventListener;
        this.eventType = eventName;
    }

    @Override
    public AsynchronousSearchState sourceState() {
        return sourceState;
    }

    @Override
    public AsynchronousSearchState targetState() {
        return targetState;
    }

    @Override
    public Class<? extends Event> eventType() {
        return eventType;
    }

    @Override
    public BiConsumer<AsynchronousSearchState, Event> onEvent() {
        return onEvent;
    }

    @Override
    public BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener() {
        return eventListener;
    }
}
