/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;

import java.util.function.BiConsumer;

public class AsynchronousSearchTransition<Event extends AsynchronousSearchContextEvent>
    implements
        Transition<AsynchronousSearchState, Event> {

    private final AsynchronousSearchState sourceState;
    private final AsynchronousSearchState targetState;
    private final BiConsumer<AsynchronousSearchState, Event> onEvent;
    private final BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener;
    private final Class<Event> eventType;

    public AsynchronousSearchTransition(
        AsynchronousSearchState sourceState,
        AsynchronousSearchState targetState,
        BiConsumer<AsynchronousSearchState, Event> onEvent,
        BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener,
        Class<Event> eventName
    ) {
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
