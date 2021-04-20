/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.GetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.PrioritizedActionListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
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

import static com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;

/**
 * Responsible for returning partial response from {@link AsynchronousSearchService}. The listener needs to wait for completion if
 * the search is still RUNNING and also try to update the keep-alive as needed within the same wait period. Response is dispatched
 * whenever both the operations complete. If the search is however not RUNNING we simply need to update keep alive either in-memory
 * or disk and invoke the response with the search response
 */
public class TransportGetAsynchronousSearchAction extends TransportAsynchronousSearchRoutingAction<GetAsynchronousSearchRequest,
        AsynchronousSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetAsynchronousSearchAction.class);
    private final ThreadPool threadPool;
    private final AsynchronousSearchService asynchronousSearchService;

    @Inject
    public TransportGetAsynchronousSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                         ActionFilters actionFilters, AsynchronousSearchService asynchronousSearchService, Client client) {
        super(transportService, clusterService, threadPool, client, GetAsynchronousSearchAction.NAME, actionFilters,
                asynchronousSearchService, GetAsynchronousSearchRequest::new, AsynchronousSearchResponse::new);
        this.threadPool = threadPool;
        this.asynchronousSearchService = asynchronousSearchService;
    }

    @Override
    public void handleRequest(AsynchronousSearchId asynchronousSearchId, GetAsynchronousSearchRequest request,
                              ActionListener<AsynchronousSearchResponse> listener, User user) {
        try {
            boolean updateNeeded = request.getKeepAlive() != null;
            if (updateNeeded) {
                asynchronousSearchService.updateKeepAliveAndGetContext(request.getId(), request.getKeepAlive(),
                        asynchronousSearchId.getAsynchronousSearchContextId(), user, ActionListener.wrap(
                                // check if the context is active and is still RUNNING
                                (context) -> handleWaitForCompletion(context, request, listener),
                                (e) -> {
                                    logger.debug(() -> new ParameterizedMessage("Unable to update and get asynchronous search request [{}]",
                                            asynchronousSearchId), e);
                                    listener.onFailure(e);
                                }
                        ));
            } else {
                // we don't need to update keep-alive, simply find one on the node if one exists or look up the index
                asynchronousSearchService.findContext(request.getId(), asynchronousSearchId.getAsynchronousSearchContextId(), user,
                        ActionListener.wrap((context) -> handleWaitForCompletion(context, request, listener),
                        (e) -> {
                            logger.debug(() -> new ParameterizedMessage("Unable to get asynchronous search [{}]",
                                    asynchronousSearchId), e);
                            listener.onFailure(e);
                        }));
            }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Unable to update and get asynchronous search [{}]", request.getId()), e);
            listener.onFailure(e);
        }
    }

    private void handleWaitForCompletion(AsynchronousSearchContext context, GetAsynchronousSearchRequest request,
                                         ActionListener<AsynchronousSearchResponse> listener) {
        //We wait for a response only if a wait for completion is non-null and the search execution is still in progress.
        if (context.isRunning() && request.getWaitForCompletionTimeout() != null) {
            logger.debug("Context is running for asynchronous search id [{}]", context.getAsynchronousSearchId());
            AsynchronousSearchProgressListener progressActionListener = context.getAsynchronousSearchProgressListener();
            assert progressActionListener != null : "progress listener cannot be null";
            PrioritizedActionListener<AsynchronousSearchResponse> wrappedListener = AsynchronousSearchTimeoutWrapper.wrapScheduledTimeout(
                    threadPool, request.getWaitForCompletionTimeout(), OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                    listener,
                    (actionListener) -> {
                        progressActionListener.searchProgressActionListener().removeListener(actionListener);
                        listener.onResponse(context.getAsynchronousSearchResponse());
                    });
            progressActionListener.searchProgressActionListener().addOrExecuteListener(wrappedListener);
        } else {
            // we don't need to wait any further on search completion
            logger.debug("Context is not running for asynchronous search id [{}]", context.getAsynchronousSearchId());
            listener.onResponse(context.getAsynchronousSearchResponse());
        }
    }
}
