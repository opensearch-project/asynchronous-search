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

package com.amazon.opendistroforelasticsearch.search.async.context;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class AsyncSearchContextId implements Writeable {

    private long id;
    private String contextId;

    public AsyncSearchContextId(String contextId, long id) {
        this.id = id;
        this.contextId = contextId;
    }

    public AsyncSearchContextId(StreamInput in) throws IOException {
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
        AsyncSearchContextId other = (AsyncSearchContextId) o;
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
