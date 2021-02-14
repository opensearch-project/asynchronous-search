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

package com.amazon.opendistroforelasticsearch.search.asynchronous.stats;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

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
