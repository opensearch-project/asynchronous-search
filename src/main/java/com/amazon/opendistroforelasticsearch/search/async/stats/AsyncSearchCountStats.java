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

package com.amazon.opendistroforelasticsearch.search.async.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Accumulates Async Search State-wise Counters stats on a single node
 */
public class AsyncSearchCountStats implements Writeable, ToXContentFragment {

    private final long runningCount;
    private final long persistedCount;
    private final long persistFailedCount;
    private final long completedCount;
    private final long failedCount;
    private final long throttledCount;

    public AsyncSearchCountStats(long runningCount, long persistedCount, long completedCount, long failedCount, long throttledCount,
                                 long persistFailedCount) {
        this.runningCount = runningCount;
        this.persistedCount = persistedCount;
        this.persistFailedCount = persistFailedCount;
        this.completedCount = completedCount;
        this.failedCount = failedCount;
        this.throttledCount = throttledCount;
    }

    public AsyncSearchCountStats(StreamInput in) throws IOException {
        this.runningCount = in.readVLong();
        this.persistedCount = in.readVLong();
        this.completedCount = in.readVLong();
        this.failedCount = in.readVLong();
        this.throttledCount = in.readVLong();
        this.persistFailedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.runningCount);
        out.writeVLong(this.persistedCount);
        out.writeVLong(this.completedCount);
        out.writeVLong(this.failedCount);
        out.writeVLong(this.throttledCount);
        out.writeVLong(this.persistFailedCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ASYNC_SEARCH_STATS);
        builder.field(Fields.RUNNING, runningCount);
        builder.field(Fields.PERSISTED, persistedCount);
        builder.field(Fields.FAILED, failedCount);
        builder.field(Fields.COMPLETED, completedCount);
        builder.field(Fields.REJECTED, throttledCount);
        builder.field(Fields.PERSIST_FAILED, persistFailedCount);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        private static final String ASYNC_SEARCH_STATS = "asynchronous_search_stats";
        private static final String RUNNING = "running_current";
        private static final String PERSISTED = "persisted";
        private static final String PERSIST_FAILED = "persist_failed";
        private static final String FAILED = "failed";
        private static final String COMPLETED = "completed";
        private static final String REJECTED = "rejected";
    }

    public long getRunningCount() {
        return runningCount;
    }

    public long getPersistedCount() {
        return persistedCount;
    }

    public long getCompletedCount() {
        return completedCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public long getThrottledCount() {
        return throttledCount;
    }
}
