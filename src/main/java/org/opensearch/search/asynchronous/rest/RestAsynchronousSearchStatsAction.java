/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.rest;

import org.opensearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import org.opensearch.search.asynchronous.request.AsynchronousSearchStatsRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin.BASE_URI;
import static org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI;

public class RestAsynchronousSearchStatsAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestAsynchronousSearchStatsAction.class);

    private static final String NAME = "asynchronous_search_stats_action";

    public RestAsynchronousSearchStatsAction() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        AsynchronousSearchStatsRequest asynchronousSearchStatsRequest = getRequest(request);

        return channel -> client.execute(
            AsynchronousSearchStatsAction.INSTANCE,
            asynchronousSearchStatsRequest,
            new RestActions.NodesResponseRestListener<>(channel)
        );
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return Arrays.asList(
            new ReplacedRoute(GET, BASE_URI + "/_nodes/{nodeId}/stats", GET, LEGACY_OPENDISTRO_BASE_URI + "/_nodes/{nodeId}/stats"),
            new ReplacedRoute(GET, BASE_URI + "/stats", GET, LEGACY_OPENDISTRO_BASE_URI + "/stats")
        );
    }

    private AsynchronousSearchStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));

        AsynchronousSearchStatsRequest asynchronousSearchStatsRequest = new AsynchronousSearchStatsRequest(nodesIds);
        asynchronousSearchStatsRequest.timeout(request.param("timeout"));
        return asynchronousSearchStatsRequest;
    }
}
