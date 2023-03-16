/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.stats;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class represents all stats the plugin keeps track of on a single node
 */
public class AsynchronousSearchStats extends BaseNodeResponse implements ToXContentFragment {

    private AsynchronousSearchCountStats asynchronousSearchCountStats;

    public AsynchronousSearchCountStats getAsynchronousSearchCountStats() {
        return asynchronousSearchCountStats;
    }

    public AsynchronousSearchStats(StreamInput in) throws IOException {
        super(in);
        asynchronousSearchCountStats = in.readOptionalWriteable(in1 -> new AsynchronousSearchCountStats(in1));
    }

    public AsynchronousSearchStats(DiscoveryNode node, @Nullable AsynchronousSearchCountStats asynchronousSearchCountStats) {
        super(node);
        this.asynchronousSearchCountStats = asynchronousSearchCountStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(asynchronousSearchCountStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (asynchronousSearchCountStats != null) {
            asynchronousSearchCountStats.toXContent(builder, params);
        }
        return builder;
    }
}
