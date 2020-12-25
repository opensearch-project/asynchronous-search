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

import org.elasticsearch.action.search.SearchTask;

/**
 * The state of the async search.
 */
public enum AsyncSearchState {

    /**
     * At the start of the search, before the {@link SearchTask} starts to run
     */
    INIT,

    /**
     * The search state actually has been started
     */
    RUNNING,

    /**
     * The search has completed successfully
     */
    SUCCEEDED,

    /**
     * The search execution has failed
     */
    FAILED,

    /**
     * The response is starting to get persisted
     */
    PERSISTING,

    /**
     * The context has been persisted to system index
     */
    PERSISTED,

    /**
     * The context has failed to persist to system index
     */
    PERSIST_FAILED,

    /**
     * The context has been deleted
     */
    DELETED;
}
