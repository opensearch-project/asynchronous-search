/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
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
 * The FSM encapsulating the lifecycle of an asynchronous search request. It contains the  list of valid Async search states and
 * the valid transitions that an {@linkplain AsynchronousSearchContext} can make.
 */
public class AsynchronousSearchStateMachine implements StateMachine<AsynchronousSearchState, AsynchronousSearchContextEvent> {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchStateMachine.class);

    private final Map<String, AsynchronousSearchTransition<? extends AsynchronousSearchContextEvent>> transitionsMap;
    private final AsynchronousSearchState initialState;
    private Set<AsynchronousSearchState> finalStates;
    private final Set<AsynchronousSearchState> states;
    private final AsynchronousSearchContextEventListener asynchronousSearchContextEventListener;

    public AsynchronousSearchStateMachine(
        final Set<AsynchronousSearchState> states,
        final AsynchronousSearchState initialState,
        AsynchronousSearchContextEventListener asynchronousSearchContextEventListener
    ) {
        super();
        this.transitionsMap = new HashMap<>();
        this.states = states;
        this.initialState = initialState;
        this.finalStates = new HashSet<>();
        this.asynchronousSearchContextEventListener = asynchronousSearchContextEventListener;
    }

    public void markTerminalStates(final Set<AsynchronousSearchState> finalStates) {
        this.finalStates = finalStates;
    }

    @Override
    public AsynchronousSearchState getInitialState() {
        return initialState;
    }

    @Override
    public Set<AsynchronousSearchState> getFinalStates() {
        return finalStates;
    }

    @Override
    public Set<AsynchronousSearchState> getStates() {
        return states;
    }

    @Override
    public Map<String, AsynchronousSearchTransition<? extends AsynchronousSearchContextEvent>> getTransitions() {
        return transitionsMap;
    }

    public void registerTransition(AsynchronousSearchTransition<? extends AsynchronousSearchContextEvent> transition) {
        transitionsMap.put(getTransitionId(transition), transition);
    }

    /**
     * Triggers transition from current state on receiving an event. Also invokes {@linkplain Transition#onEvent()} and
     * {@linkplain Transition#eventListener()}.
     *
     * @param event to fire
     * @return The final Async search state
     * @throws AsynchronousSearchStateMachineClosedException the state machine has reached a terminal state
     */
    @Override
    public AsynchronousSearchState trigger(AsynchronousSearchContextEvent event) throws AsynchronousSearchStateMachineClosedException {
        AsynchronousSearchContext asynchronousSearchContext = event.asynchronousSearchContext();
        synchronized (asynchronousSearchContext) {
            AsynchronousSearchState currentState = asynchronousSearchContext.getAsynchronousSearchState();
            if (getFinalStates().contains(currentState)) {
                throw new AsynchronousSearchStateMachineClosedException(currentState, event);
            }
            String transitionId = getTransitionId(currentState, event.getClass());
            if (transitionsMap.containsKey(transitionId)) {
                AsynchronousSearchTransition<? extends AsynchronousSearchContextEvent> transition = transitionsMap.get(transitionId);
                execute(transition.onEvent(), event, currentState);
                asynchronousSearchContext.setState(transition.targetState());
                logger.debug(
                    "Executed event [{}] for asynchronous search id [{}] ",
                    event.getClass().getName(),
                    event.asynchronousSearchContext.getAsynchronousSearchId()
                );
                BiConsumer<AsynchronousSearchContextId, AsynchronousSearchContextEventListener> eventListener = transition.eventListener();
                try {
                    eventListener.accept(event.asynchronousSearchContext().getContextId(), asynchronousSearchContextEventListener);
                } catch (Exception ex) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Failed to execute listener for asynchronous search id : [{}]",
                            event.asynchronousSearchContext.getAsynchronousSearchId()
                        ),
                        ex
                    );
                }
                return asynchronousSearchContext.getAsynchronousSearchState();
            } else {
                String message = String.format(
                    Locale.ROOT,
                    "Invalid transition for " + "asynchronous search context [%s] from source state [%s] on event [%s]",
                    asynchronousSearchContext.getAsynchronousSearchId(),
                    currentState,
                    event.getClass().getName()
                );
                logger.error(message);
                throw new IllegalStateException(message);
            }
        }
    }

    @SuppressWarnings("unchecked")
    // Suppress the warning since we know the type of the event and transition based on the validation
    private <T> void execute(
        BiConsumer<AsynchronousSearchState, T> onEvent,
        AsynchronousSearchContextEvent event,
        AsynchronousSearchState state
    ) {
        onEvent.accept(state, (T) event);
    }

    /**
     * @param transition The asynchronous search transition
     * @return an identifier capturing information regarding the source state and the event type which acts as key for transition map
     */

    public String getTransitionId(AsynchronousSearchTransition<? extends AsynchronousSearchContextEvent> transition) {
        return getTransitionId(transition.sourceState(), transition.eventType());
    }

    /**
     * @param sourceState current state of context
     * @param eventType   type of the asynchronous search context event subclass
     * @return an identifier capturing information regarding the source state and the event type which acts as key for transition map
     */
    private String getTransitionId(AsynchronousSearchState sourceState, Class<?> eventType) {
        return sourceState + "_" + eventType;
    }

}
