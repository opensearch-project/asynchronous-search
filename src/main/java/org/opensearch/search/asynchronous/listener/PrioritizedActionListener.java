/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.listener;


import org.opensearch.action.ActionListener;

public interface PrioritizedActionListener<Response> extends ActionListener<Response> {

    void executeImmediately();
}
