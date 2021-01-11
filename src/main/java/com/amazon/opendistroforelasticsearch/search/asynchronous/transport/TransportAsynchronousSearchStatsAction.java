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

package com.amazon.opendistroforelasticsearch.search.asynchronous.transport;

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.AsynchronousSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchStatsResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.AsynchronousSearchStats;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportAsynchronousSearchStatsAction
        extends TransportNodesAction<AsynchronousSearchStatsRequest, AsynchronousSearchStatsResponse,
        TransportAsynchronousSearchStatsAction.AsynchronousSearchStatsNodeRequest, AsynchronousSearchStats> {

    private final AsynchronousSearchService asynchronousSearchService;

    @Inject
    public TransportAsynchronousSearchStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                           ActionFilters actionFilters, AsynchronousSearchService asynchronousSearchService) {

        super(AsynchronousSearchStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                AsynchronousSearchStatsRequest::new, AsynchronousSearchStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT,
                AsynchronousSearchStats.class);
        this.asynchronousSearchService = asynchronousSearchService;

    }

    @Override
    protected AsynchronousSearchStatsResponse newResponse(AsynchronousSearchStatsRequest request, List<AsynchronousSearchStats> responses,
                                                   List<FailedNodeException> failures) {
        return new AsynchronousSearchStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected AsynchronousSearchStatsNodeRequest newNodeRequest(AsynchronousSearchStatsRequest request) {
        return new AsynchronousSearchStatsNodeRequest(request);
    }

    @Override
    protected AsynchronousSearchStats newNodeResponse(StreamInput in) throws IOException {
        return new AsynchronousSearchStats(in);
    }

    @Override
    protected AsynchronousSearchStats nodeOperation(AsynchronousSearchStatsNodeRequest asynchronousSearchStatsNodeRequest) {
        return asynchronousSearchService.stats();

    }

    /**
     * Request to fetch asynchronous search stats on a single node
     */
    public static class AsynchronousSearchStatsNodeRequest extends BaseNodeRequest {

        AsynchronousSearchStatsRequest request;

        public AsynchronousSearchStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new AsynchronousSearchStatsRequest(in);
        }

        AsynchronousSearchStatsNodeRequest(AsynchronousSearchStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
