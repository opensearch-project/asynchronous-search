/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.response;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.asynchronous.stats.AsynchronousSearchStats;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

/**
 * Contains aggregation of asynchronous search stats from all the nodes
 */
public class AsynchronousSearchStatsResponse extends BaseNodesResponse<AsynchronousSearchStats> implements ToXContentObject {
    public AsynchronousSearchStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public AsynchronousSearchStatsResponse(ClusterName clusterName, List<AsynchronousSearchStats> nodes,
                                           List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<AsynchronousSearchStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(new Reader<AsynchronousSearchStats>() {
            @Override
            public AsynchronousSearchStats read(StreamInput in1) throws IOException {
                return new AsynchronousSearchStats(in1);
            }
        });
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<AsynchronousSearchStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (AsynchronousSearchStats stats : getNodes()) {
            builder.startObject(stats.getNode().getId());
            stats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
