/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Accumulates Async Search State-wise Counters stats on a single node
 */
public class AsynchronousSearchCountStats implements Writeable, ToXContentFragment {

    private final long runningCount;
    private final long persistedCount;
    private final long persistFailedCount;
    private final long completedCount;
    private final long failedCount;
    private final long throttledCount;
    private final long initializedCount;
    private final long cancelledCount;
    private final long submittedCount;

    public AsynchronousSearchCountStats(long runningCount, long persistedCount, long completedCount, long failedCount, long throttledCount,
                                 long persistFailedCount, long initializedCount, long submittedCount, long cancelledCount) {
        this.runningCount = runningCount;
        this.persistedCount = persistedCount;
        this.persistFailedCount = persistFailedCount;
        this.completedCount = completedCount;
        this.failedCount = failedCount;
        this.throttledCount = throttledCount;
        this.initializedCount = initializedCount;
        this.cancelledCount = cancelledCount;
        this.submittedCount = submittedCount;
    }

    public AsynchronousSearchCountStats(StreamInput in) throws IOException {
        this.runningCount = in.readVLong();
        this.persistedCount = in.readVLong();
        this.completedCount = in.readVLong();
        this.failedCount = in.readVLong();
        this.throttledCount = in.readVLong();
        this.persistFailedCount = in.readVLong();
        this.initializedCount = in.readVLong();
        this.cancelledCount = in.readVLong();
        this.submittedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.runningCount);
        out.writeVLong(this.persistedCount);
        out.writeVLong(this.completedCount);
        out.writeVLong(this.failedCount);
        out.writeVLong(this.throttledCount);
        out.writeVLong(this.persistFailedCount);
        out.writeVLong(this.initializedCount);
        out.writeVLong(this.cancelledCount);
        out.writeVLong(this.submittedCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ASYNC_SEARCH_STATS);
        builder.field(Fields.SUBMITTED, submittedCount);
        builder.field(Fields.INITIALIZED, initializedCount);
        builder.field(Fields.RUNNING, runningCount);
        builder.field(Fields.PERSISTED, persistedCount);
        builder.field(Fields.FAILED, failedCount);
        builder.field(Fields.COMPLETED, completedCount);
        builder.field(Fields.REJECTED, throttledCount);
        builder.field(Fields.PERSIST_FAILED, persistFailedCount);
        builder.field(Fields.CANCELLED, cancelledCount);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        private static final String ASYNC_SEARCH_STATS = "asynchronous_search_stats";
        private static final String RUNNING = "running_current";
        private static final String PERSISTED = "persisted";
        private static final String PERSIST_FAILED = "persist_failed";
        private static final String FAILED = "search_failed";
        private static final String COMPLETED = "search_completed";
        private static final String REJECTED = "rejected";
        private static final String SUBMITTED = "submitted";
        private static final String INITIALIZED = "initialized";
        private static final String CANCELLED = "cancelled";
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

    public long getInitializedCount() {
        return initializedCount;
    }

    public long getCancelledCount() {
        return cancelledCount;
    }

    public long getSubmittedCount() {
        return submittedCount;
    }
}
