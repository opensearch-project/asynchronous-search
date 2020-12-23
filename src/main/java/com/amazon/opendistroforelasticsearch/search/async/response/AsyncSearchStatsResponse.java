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

package com.amazon.opendistroforelasticsearch.search.async.response;

import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

/**
 * Contains aggregation of async search stats from all the nodes
 */
public class AsyncSearchStatsResponse extends BaseNodesResponse<AsyncSearchStats> implements ToXContentObject {
    public AsyncSearchStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public AsyncSearchStatsResponse(ClusterName clusterName, List<AsyncSearchStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<AsyncSearchStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(new Reader<AsyncSearchStats>() {
            @Override
            public AsyncSearchStats read(StreamInput in1) throws IOException {
                return new AsyncSearchStats(in1);
            }
        });
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<AsyncSearchStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (AsyncSearchStats stats : getNodes()) {
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
