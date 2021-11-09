/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.state;

import java.util.Map;
import java.util.Set;

/**
 * {@linkplain StateMachine} provides APIs for generic finite state machine needed
 * for basic operations like working with states, events and a lifecycle.
 *
 * @param <State> the type of state
 * @param <Event> the type of event
 */
interface StateMachine<State, Event> {

    /**
     * Return FSM initial state.
     *
     * @return FSM initial state
     */
    State getInitialState();

    /**
     * Return FSM final states.
     *
     * @return FSM final states
     */
    Set<State> getFinalStates();

    /**
     * Return FSM registered states.
     *
     * @return FSM registered states
     */
    Set<State> getStates();

    /**
     * Return FSM registered transitions.
     *
     * @return FSM registered transitions
     */
    Map<String, ? extends Transition<State, ? extends Event>> getTransitions();

    /**
     * Fire an event. According to event type, the FSM will make the right transition.
     *
     * @param event to fire
     * @return The next FSM state defined by the transition to make
     * @throws Exception thrown if an exception occurs during event handling
     */
    State trigger(Event event) throws Exception;

}
