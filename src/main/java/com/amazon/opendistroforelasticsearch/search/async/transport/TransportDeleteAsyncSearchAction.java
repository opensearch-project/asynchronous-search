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

package com.amazon.opendistroforelasticsearch.search.async.transport;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for deleting an async search request
 */
public class TransportDeleteAsyncSearchAction extends TransportAsyncSearchRoutingAction<DeleteAsyncSearchRequest, AcknowledgedResponse> {

    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportDeleteAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, AsyncSearchService asyncSearchService, Client client) {
        super(transportService, clusterService, threadPool, client, DeleteAsyncSearchAction.NAME, actionFilters,
                DeleteAsyncSearchRequest::new, AcknowledgedResponse::new);
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, DeleteAsyncSearchRequest request,
                              ActionListener<AcknowledgedResponse> listener, User user) {
        try {
            asyncSearchService.freeContext(request.getId(), asyncSearchId.getAsyncSearchContextId(), user, ActionListener
                    .wrap((complete) -> listener.onResponse(new AcknowledgedResponse(complete)), listener::onFailure));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Unable to delete async search [{}]", request.getId()), e);
            listener.onFailure(e);
        }
    }
}
