/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.id;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;

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
