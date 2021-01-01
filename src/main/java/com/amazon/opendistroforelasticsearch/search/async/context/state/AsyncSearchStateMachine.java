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

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextEventListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * The FSM encapsulating the lifecycle of an async search request. It contains the  list of valid Async search states and
 * the valid transitions that an {@linkplain AsyncSearchContext} can make.
 */
public class AsyncSearchStateMachine implements StateMachine<AsyncSearchState, AsyncSearchContextEvent> {

    private static final Logger logger = LogManager.getLogger(AsyncSearchStateMachine.class);

    private final Map<String, AsyncSearchTransition<? extends AsyncSearchContextEvent>> transitionsMap;
    private final AsyncSearchState initialState;
    private Set<AsyncSearchState> finalStates;
    private final Set<AsyncSearchState> states;
    private final AsyncSearchContextEventListener asyncSearchContextEventListener;

    public AsyncSearchStateMachine(final Set<AsyncSearchState> states, final AsyncSearchState initialState,
                                   AsyncSearchContextEventListener asyncSearchContextEventListener) {
        super();
        this.transitionsMap = new HashMap<>();
        this.states = states;
        this.initialState = initialState;
        this.finalStates = new HashSet<>();
        this.asyncSearchContextEventListener = asyncSearchContextEventListener;
    }

    public void markTerminalStates(final Set<AsyncSearchState> finalStates) {
        this.finalStates = finalStates;
    }

    @Override
    public AsyncSearchState getInitialState() {
        return initialState;
    }

    @Override
    public Set<AsyncSearchState> getFinalStates() {
        return finalStates;
    }

    @Override
    public Set<AsyncSearchState> getStates() {
        return states;
    }

    @Override
    public Map<String, AsyncSearchTransition<? extends AsyncSearchContextEvent>> getTransitions() {
        return transitionsMap;
    }

    public void registerTransition(AsyncSearchTransition<? extends AsyncSearchContextEvent> transition) {
        transitionsMap.put(getTransitionId(transition), transition);
    }

    /**
     * Triggers transition from current state on receiving an event. Also invokes {@linkplain Transition#onEvent()} and
     * {@linkplain Transition#eventListener()}.
     *
     * @param event to fire
     * @return The final Async search state
     * @throws AsyncSearchStateMachineClosedException the state machine has reached a terminal state
     */
    @Override
    public AsyncSearchState trigger(AsyncSearchContextEvent event) throws AsyncSearchStateMachineClosedException {
        AsyncSearchContext asyncSearchContext = event.asyncSearchContext();
        synchronized (asyncSearchContext) {
            AsyncSearchState currentState = asyncSearchContext.getAsyncSearchState();
            if (getFinalStates().contains(currentState)) {
                throw new AsyncSearchStateMachineClosedException(currentState, event);
            }
            String transitionId = getTransitionId(currentState, event.getClass());
            if (transitionsMap.containsKey(transitionId)) {
                AsyncSearchTransition<? extends AsyncSearchContextEvent> transition = transitionsMap.get(transitionId);
                execute(transition.onEvent(), event, currentState);
                asyncSearchContext.setState(transition.targetState());
                logger.debug("Executed event [{}] for async search id [{}] ", event.getClass().getName(),
                        event.asyncSearchContext.getAsyncSearchId());
                BiConsumer<AsyncSearchContextId, AsyncSearchContextEventListener> eventListener = transition.eventListener();
                try {
                    eventListener.accept(event.asyncSearchContext().getContextId(), asyncSearchContextEventListener);
                } catch (Exception ex) {
                    logger.error(() -> new ParameterizedMessage("Failed to execute listener for async search id : [{}]",
                            event.asyncSearchContext.getAsyncSearchId()), ex);
                }
                return asyncSearchContext.getAsyncSearchState();
            } else {
                String message = String.format(Locale.ROOT, "Invalid transition for async search context [%s] from source state [%s] on " +
                                "event [%s]",
                        asyncSearchContext.getAsyncSearchId(), currentState, event.getClass().getName());
                logger.error(message);
                throw new IllegalStateException(message);
            }
        }
    }


    @SuppressWarnings("unchecked")
    //Suppress the warning since we know the type of the event and transition based on the validation
    private <T> void execute(BiConsumer<AsyncSearchState, T> onEvent, AsyncSearchContextEvent event, AsyncSearchState state) {
        onEvent.accept(state, (T) event);
    }

    /**
     * @param transition The async search transition
     * @return an identifier capturing information regarding the source state and the event type which acts as key for transition map
     */

    public String getTransitionId(AsyncSearchTransition<? extends AsyncSearchContextEvent> transition) {
        return getTransitionId(transition.sourceState(), transition.eventType());
    }

    /**
     * @param sourceState current state of context
     * @param eventType   type of the async search context event subclass
     * @return an identifier capturing information regarding the source state and the event type which acts as key for transition map
     */
    private String getTransitionId(AsyncSearchState sourceState, Class<?> eventType) {
        return sourceState + "_" + eventType;
    }

}

