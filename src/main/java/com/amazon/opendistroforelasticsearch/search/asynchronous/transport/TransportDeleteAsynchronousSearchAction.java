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

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for deleting an asynchronous search request
 */
public class TransportDeleteAsynchronousSearchAction extends TransportAsynchronousSearchRoutingAction<DeleteAsynchronousSearchRequest,
        AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAsynchronousSearchRoutingAction.class);

    private final AsynchronousSearchService asynchronousSearchService;

    @Inject
    public TransportDeleteAsynchronousSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, AsynchronousSearchService asynchronousSearchService,
                                                   Client client) {
        super(transportService, clusterService, threadPool, client, DeleteAsynchronousSearchAction.NAME, actionFilters,
                asynchronousSearchService, DeleteAsynchronousSearchRequest::new, AcknowledgedResponse::new);
        this.asynchronousSearchService = asynchronousSearchService;
    }

    @Override
    public void handleRequest(AsynchronousSearchId asynchronousSearchId, DeleteAsynchronousSearchRequest request,
                              ActionListener<AcknowledgedResponse> listener, User user) {
        try {
            asynchronousSearchService.freeContext(request.getId(), asynchronousSearchId.getAsynchronousSearchContextId(), user,
                    ActionListener.wrap((complete) -> listener.onResponse(new AcknowledgedResponse(complete)), listener::onFailure));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Unable to delete asynchronous search [{}]", request.getId()), e);
            listener.onFailure(e);
        }
    }
}
