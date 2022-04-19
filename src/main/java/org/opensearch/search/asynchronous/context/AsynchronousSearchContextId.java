/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class AsynchronousSearchContextId implements Writeable {

    private long id;
    private String contextId;

    public AsynchronousSearchContextId(String contextId, long id) {
        this.id = id;
        this.contextId = contextId;
    }

    public AsynchronousSearchContextId(StreamInput in) throws IOException {
        this.id = in.readLong();
        this.contextId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        out.writeString(contextId);
    }

    public String getContextId() {
        return contextId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AsynchronousSearchContextId other = (AsynchronousSearchContextId) o;
        return id == other.id && contextId.equals(other.contextId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextId, id);
    }

    @Override
    public String toString() {
        return "[" + contextId + "][" + id + "]";
    }

}
