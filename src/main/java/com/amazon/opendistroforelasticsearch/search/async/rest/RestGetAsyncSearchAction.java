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

import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchRoutingRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetAsyncSearchAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "get_async_search";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(GET, AsyncSearchPlugin.BASE_URI + "/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetAsyncSearchRequest getRequest = new GetAsyncSearchRequest(request.param("id"));
        if (request.hasParam("wait_for_completion_timeout")) {
            getRequest.setWaitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout",
                    GetAsyncSearchRequest.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT));
        }
        if (request.hasParam("keep_alive")) {
            getRequest.setKeepAlive(request.paramAsTime("keep_alive", null));
        }
        if (request.hasParam("connection_timeout")) {
            getRequest.connectionTimeout(request.paramAsTime("connection_timeout", AsyncSearchRoutingRequest.DEFAULT_CONNECTION_TIMEOUT));
        }
        return channel -> {
            client.execute(GetAsyncSearchAction.INSTANCE, getRequest, new RestStatusToXContentListener<>(channel));
        };
    }
}
