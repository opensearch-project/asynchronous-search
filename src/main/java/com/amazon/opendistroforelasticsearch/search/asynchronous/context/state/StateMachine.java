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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.state;

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
