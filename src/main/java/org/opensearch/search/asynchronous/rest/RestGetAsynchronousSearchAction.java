/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.rest;

import org.opensearch.search.asynchronous.action.GetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetAsynchronousSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_asynchronous_search";
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return Collections.singletonList(
            new ReplacedRoute(
                GET,
                AsynchronousSearchPlugin.BASE_URI + "/{id}",
                GET,
                AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI + "/{id}"
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetAsynchronousSearchRequest getRequest = new GetAsynchronousSearchRequest(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            getRequest.setWaitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout", null));
        }
        if (request.hasParam("keep_alive")) {
            getRequest.setKeepAlive(request.paramAsTime("keep_alive", null));
        }
        return channel -> {
            client.execute(GetAsynchronousSearchAction.INSTANCE, getRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
