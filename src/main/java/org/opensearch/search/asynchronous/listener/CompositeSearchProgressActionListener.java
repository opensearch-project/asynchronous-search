/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.listener;

import org.opensearch.core.action.ActionListener;
import org.opensearch.action.search.SearchProgressActionListener;

import java.util.ArrayList;
import java.util.List;

/***
 * The implementation of {@link SearchProgressActionListener} responsible for maintaining a list of {@link PrioritizedActionListener}
 * to be invoked when a full response is available. The implementation guarantees that the listener once added will exactly be
 * invoked once. If the search completes before the listener was added, the listener is immediately invoked
 **/

public class CompositeSearchProgressActionListener<T> implements ActionListener<T> {

    private final List<ActionListener<T>> actionListeners;
    private volatile boolean complete;

    CompositeSearchProgressActionListener() {
        this.actionListeners = new ArrayList<>(1);
    }

    /***
     * Adds a prioritized listener to listen on to the progress of the search started by a previous request. If the search
     * has completed the timeout consumer is immediately invoked.
     * @param listener the listener
     */
    public void addOrExecuteListener(PrioritizedActionListener<T> listener) {
        if (addListener(listener) == false) {
            listener.executeImmediately();
        }
    }

    public synchronized void removeListener(ActionListener<T> listener) {
        this.actionListeners.remove(listener);
    }

    private synchronized boolean addListener(ActionListener<T> listener) {
        if (complete == false) {
            this.actionListeners.add(listener);
            return true;
        }
        return false;
    }

    @Override
    public void onResponse(T response) {
        Iterable<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
        if (actionListenersToBeInvoked != null) {
            ActionListener.onResponse(actionListenersToBeInvoked, response);
        }
    }

    @Override
    public void onFailure(Exception exception) {
        Iterable<ActionListener<T>> actionListenersToBeInvoked = finalizeListeners();
        if (actionListenersToBeInvoked != null) {
            ActionListener.onFailure(actionListenersToBeInvoked, exception);
        }
    }

    private Iterable<ActionListener<T>> finalizeListeners() {
        List<ActionListener<T>> actionListenersToBeInvoked = null;
        synchronized (this) {
            if (complete == false) {
                actionListenersToBeInvoked = new ArrayList<>(actionListeners);
                actionListeners.clear();
                complete = true;
            }
        }
        return actionListenersToBeInvoked;
    }
}
