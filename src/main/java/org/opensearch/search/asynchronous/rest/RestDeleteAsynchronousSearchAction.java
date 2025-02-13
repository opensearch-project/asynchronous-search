/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.rest;

import org.opensearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAsynchronousSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_asynchronous_search";
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return Collections.singletonList(
            new ReplacedRoute(
                DELETE,
                AsynchronousSearchPlugin.BASE_URI + "/{id}",
                DELETE,
                AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI + "/{id}"
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteAsynchronousSearchRequest deleteRequest = new DeleteAsynchronousSearchRequest(request.param("id"));
        return channel -> {
            client.execute(DeleteAsynchronousSearchAction.INSTANCE, deleteRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
