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

package com.amazon.opendistroforelasticsearch.search.async.id;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import java.util.Objects;

/**
 * Holds the following details pertaining to a submitted async search:
 * - the associated async search context
 * - id of the associated search task registered with the task manager
 * - the id of node on which acts as coordinator for the async search
 */
public final class AsyncSearchId {

    // UUID + ID generator for uniqueness
    private final AsyncSearchContextId asyncSearchContextId;
    // coordinator node id
    private final String node;
    // the search task id
    private final long taskId;

    public AsyncSearchId(String node, long taskId, AsyncSearchContextId asyncSearchContextId) {
        this.node = node;
        this.taskId = taskId;
        this.asyncSearchContextId = asyncSearchContextId;
    }

    public AsyncSearchContextId getAsyncSearchContextId() {
        return asyncSearchContextId;
    }

    public String getNode() {
        return node;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return "[" + node + "][" + taskId + "][" + asyncSearchContextId + "]";
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.asyncSearchContextId, this.node, this.taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsyncSearchId asyncSearchId = (AsyncSearchId) o;
        return asyncSearchId.asyncSearchContextId.equals(this.asyncSearchContextId)
                && asyncSearchId.node.equals(this.node)
                && asyncSearchId.taskId == this.taskId;
    }
}
