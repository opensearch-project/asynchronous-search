/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state;

import java.util.function.BiConsumer;

public interface Transition<State, Event> {

    /**
     * Return transition source state.
     *
     * @return transition source state
     */
    State sourceState();

    /**
     * Return transition target state.
     *
     * @return transition target state
     */
    State targetState();

    /**
     * the event type
     * @return event type
     */

    Class<? extends Event> eventType();

    /**
     * Return a consumer to execute when an event is fired.
     *
     * @return a bi-consumer
     */
    BiConsumer<State, Event> onEvent();

    /**
     * Event listener to be invoked on transition
     *
     * @return the event listener
     */

    BiConsumer<?, ?> eventListener();

}
