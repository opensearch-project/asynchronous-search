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

package com.amazon.opendistroforelasticsearch.search.asynchronous.listener;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;

/**
 * An listener for asynchronous search context events.
 */
public interface AsynchronousSearchContextEventListener {

    /**
     * @param contextId Executed when a new asynchronous search context was created
     */
    default void onNewContext(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context completes.
     */
    default void onContextCompleted(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context fails.
     */
    default void onContextFailed(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context is persisted.
     */
    default void onContextPersisted(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context fails persisting.
     */
    default void onContextPersistFailed(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context is deleted.
     */
    default void onContextDeleted(AsynchronousSearchContextId contextId) {
    }

    /**
     * @param contextId Executed when a previously created asynchronous search context is running.
     */
    default void onContextRunning(AsynchronousSearchContextId contextId) {

    }

    /**
     * @param contextId Executed when asynchronous search context creation is rejected
     */
    default void onContextRejected(AsynchronousSearchContextId contextId) {

    }

    /**
     * @param contextId Executed when a running asynchronous search context is deleted and has bypassed succeeded/failed state
     */
    default void onRunningContextDeleted(AsynchronousSearchContextId contextId) {

    }

    /**
     * @param contextId Executed when an asynchronous search context is cancelled
     */
    default void onContextCancelled(AsynchronousSearchContextId contextId) {

    }

    /**
     * @param contextId Executed when an asynchronous search context is initialized
     */
    default void onContextInitialized(AsynchronousSearchContextId contextId) {

    }
}
