/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
