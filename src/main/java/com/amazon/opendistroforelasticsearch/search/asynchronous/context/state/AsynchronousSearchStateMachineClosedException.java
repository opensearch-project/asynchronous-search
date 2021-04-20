/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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

import java.util.Locale;

public class AsynchronousSearchStateMachineClosedException extends Exception {

    private final AsynchronousSearchState currentState;
    private final AsynchronousSearchContextEvent contextEvent;

    public AsynchronousSearchStateMachineClosedException(AsynchronousSearchState currentState,
                                                         AsynchronousSearchContextEvent contextEvent) {
        super(String.format(Locale.ROOT, "Invalid transition for CLOSED context [%s] from source state [%s] on event [%s]",
                contextEvent.asynchronousSearchContext.getAsynchronousSearchId(), currentState, contextEvent.getClass().getName()));
        this.currentState = currentState;
        this.contextEvent = contextEvent;
    }

    public AsynchronousSearchState getCurrentState() {
        return currentState;
    }

    public AsynchronousSearchContextEvent getContextEvent() {
        return contextEvent;
    }
}
