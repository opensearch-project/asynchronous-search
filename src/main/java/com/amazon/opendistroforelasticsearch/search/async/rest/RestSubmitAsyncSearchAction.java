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

import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSubmitAsyncSearchAction extends BaseRestHandler {
    /**
     * Indicates whether hits.total should be rendered as an integer or an object
     * in the rest search response.
     */
    public static final String TOTAL_HITS_AS_INT_PARAM = "rest_total_hits_as_int";
    public static final String TYPED_KEYS_PARAM = "typed_keys";
    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList(TYPED_KEYS_PARAM, TOTAL_HITS_AS_INT_PARAM));
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    public String getName() {
        return "async_search_action";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(
                new Route(POST, AsyncSearchPlugin.BASE_URI),
                new Route(POST, "/{index}" + AsyncSearchPlugin.BASE_URI));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        IntConsumer setSize = size -> searchRequest.source().size(size);
        request.withContentOrSourceParamParserOrNull(parser ->
                RestSearchAction.parseSearchRequest(searchRequest, request, parser, setSize));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        if (request.hasParam("wait_for_completion_timeout")) {
            submitAsyncSearchRequest.waitForCompletionTimeout(request.paramAsTime("wait_for_completion_timeout",
                    SubmitAsyncSearchRequest.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT));
        }
        if (request.hasParam("keep_alive")) {
            submitAsyncSearchRequest.keepAlive(request.paramAsTime("keep_alive", SubmitAsyncSearchRequest.DEFAULT_KEEP_ALIVE));
        }
        if (request.hasParam("keep_on_completion")) {
            submitAsyncSearchRequest.keepOnCompletion(request.paramAsBoolean("keep_on_completion",
                    SubmitAsyncSearchRequest.DEFAULT_KEEP_ON_COMPLETION));
        }
        if (request.hasParam("ccs_minimize_roundtrips")) {
            searchRequest.setCcsMinimizeRoundtrips(false);
        }
        if (request.hasParam("pre_filter_shard_size")) {
            searchRequest.setPreFilterShardSize(request.paramAsInt("pre_filter_shard_size",
                    SubmitAsyncSearchRequest.DEFAULT_PRE_FILTER_SHARD_SIZE));
        }
        if (request.hasParam("request_cache")) {
            searchRequest.requestCache(false);
        }
        if (request.hasParam("batched_reduce_size")) {
            final int batchedReduceSize = request.paramAsInt("batched_reduce_size",
                    SubmitAsyncSearchRequest.DEFAULT_BATCHED_REDUCE_SIZE);
            searchRequest.setBatchedReduceSize(batchedReduceSize);
        }
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest, new RestStatusToXContentListener<>(channel));
        };
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}
