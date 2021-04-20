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

package com.amazon.opendistroforelasticsearch.search.asynchronous.response;

import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.AsynchronousSearchStats;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
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
