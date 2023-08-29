/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.transport;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.action.ActionListener;
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
