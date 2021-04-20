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

package com.amazon.opendistroforelasticsearch.search.asynchronous.id;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import java.util.Objects;

/**
 * Holds the following details pertaining to a submitted asynchronous search:
 * - the associated asynchronous search context
 * - id of the associated search task registered with the task manager
 * - the id of node on which acts as coordinator for the asynchronous search
 */
public final class AsynchronousSearchId {

    // UUID + ID generator for uniqueness
    private final AsynchronousSearchContextId asynchronousSearchContextId;
    // coordinator node id
    private final String node;
    // the search task id
    private final long taskId;

    public AsynchronousSearchId(String node, long taskId, AsynchronousSearchContextId asynchronousSearchContextId) {
        this.node = node;
        this.taskId = taskId;
        this.asynchronousSearchContextId = asynchronousSearchContextId;
    }

    public AsynchronousSearchContextId getAsynchronousSearchContextId() {
        return asynchronousSearchContextId;
    }

    public String getNode() {
        return node;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return "[" + node + "][" + taskId + "][" + asynchronousSearchContextId + "]";
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.asynchronousSearchContextId, this.node, this.taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsynchronousSearchId asynchronousSearchId = (AsynchronousSearchId) o;
        return asynchronousSearchId.asynchronousSearchContextId.equals(this.asynchronousSearchContextId)
                && asynchronousSearchId.node.equals(this.node)
                && asynchronousSearchId.taskId == this.taskId;
    }
}
