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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;

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
