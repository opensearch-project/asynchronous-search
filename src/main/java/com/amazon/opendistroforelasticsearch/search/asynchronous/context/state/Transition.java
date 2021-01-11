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
