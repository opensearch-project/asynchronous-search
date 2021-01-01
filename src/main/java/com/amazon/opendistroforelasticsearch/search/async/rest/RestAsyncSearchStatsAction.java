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

package com.amazon.opendistroforelasticsearch.search.async.rest;

import com.amazon.opendistroforelasticsearch.search.async.action.AsyncSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchStatsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestAsyncSearchStatsAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestAsyncSearchStatsAction.class);

    private static final String NAME = "async_search_stats_action";

    public RestAsyncSearchStatsAction() {
    }

    @Override
    public String getName() {
        return NAME;
    }


    @Override
    public List<Route> routes() {
        return Arrays.asList(
                new Route(GET, AsyncSearchPlugin.BASE_URI + "/_nodes/{nodeId}/_stats"),
                new Route(GET, AsyncSearchPlugin.BASE_URI + "/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AsyncSearchStatsRequest asyncSearchStatsRequest = getRequest(request);

        return channel -> client.execute(AsyncSearchStatsAction.INSTANCE, asyncSearchStatsRequest,
                new RestActions.NodesResponseRestListener<>(channel));
    }

    private AsyncSearchStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));

        AsyncSearchStatsRequest asyncSearchStatsRequest = new AsyncSearchStatsRequest(nodesIds);
        asyncSearchStatsRequest.timeout(request.param("timeout"));
        return asyncSearchStatsRequest;
    }
}

