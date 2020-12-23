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

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchRoutingRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteAsyncSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_async_search";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(DELETE, AsyncSearchPlugin.BASE_URI + "/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        DeleteAsyncSearchRequest deleteRequest = new DeleteAsyncSearchRequest(request.param("id"));
        if (request.hasParam("connection_timeout")) {
            deleteRequest.connectionTimeout(request.paramAsTime("connection_timeout",
                    AsyncSearchRoutingRequest.DEFAULT_CONNECTION_TIMEOUT));
        }
        return channel -> {
            client.execute(DeleteAsyncSearchAction.INSTANCE, deleteRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
