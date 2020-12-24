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
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.async.listener.PrioritizedActionListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Responsible for returning partial response from {@link AsyncSearchService}. The listener needs to wait for completion if
 * the search is still RUNNING and also try to update the keep-alive as needed within the same wait period. Response is dispatched
 * whenever both the operations complete. If the search is however not RUNNING we simply need to update keep alive either in-memory
 * or disk and invoke the response with the search response
 */
public class TransportGetAsyncSearchAction extends TransportAsyncSearchRoutingAction<GetAsyncSearchRequest, AsyncSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetAsyncSearchAction.class);
    private final ThreadPool threadPool;
    private final AsyncSearchService asyncSearchService;

    @Inject
    public TransportGetAsyncSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                         ActionFilters actionFilters, AsyncSearchService asyncSearchService, Client client) {
        super(transportService, clusterService, threadPool, client, GetAsyncSearchAction.NAME, actionFilters, GetAsyncSearchRequest::new,
                AsyncSearchResponse::new);
        this.threadPool = threadPool;
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    public void handleRequest(AsyncSearchId asyncSearchId, GetAsyncSearchRequest request,
                              ActionListener<AsyncSearchResponse> listener, User user) {
        try {
            boolean updateNeeded = request.getKeepAlive() != null;
            if (updateNeeded) {
                asyncSearchService.updateKeepAliveAndGetContext(request.getId(), request.getKeepAlive(),
                        asyncSearchId.getAsyncSearchContextId(), user, ActionListener.wrap(
                                // check if the context is active and is still RUNNING
                                (context) -> handleWaitForCompletion(context, request.getWaitForCompletionTimeout(), listener),
                                (e) -> {
                                    logger.debug(() -> new ParameterizedMessage("Unable to update and get async search request {}",
                                            asyncSearchId), e);
                                    listener.onFailure(e);
                                }
                        ));
            } else {
                // we don't need to update keep-alive, simply find one on the node if one exists or look up the index
                asyncSearchService.findContext(request.getId(), asyncSearchId.getAsyncSearchContextId(), user, ActionListener.wrap(
                        (context) -> handleWaitForCompletion(context, request.getWaitForCompletionTimeout(), listener),
                        (e) -> {
                            logger.debug(() -> new ParameterizedMessage("Unable to get async search request {}",
                                    asyncSearchId), e);
                            listener.onFailure(e);
                        }));
            }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Unable to update and get async search request {}", request), e);
            listener.onFailure(e);
        }
    }

    private void handleWaitForCompletion(AsyncSearchContext context, TimeValue timeValue, ActionListener<AsyncSearchResponse> listener) {
        if (context.isRunning()) {
            logger.debug("Context is running for async search id [{}]", context.getAsyncSearchId());
            AsyncSearchProgressListener progressActionListener = context.getAsyncSearchProgressListener();
            assert progressActionListener != null : "progress listener cannot be null";
            PrioritizedActionListener<AsyncSearchResponse> wrappedListener = AsyncSearchTimeoutWrapper.wrapScheduledTimeout(threadPool,
                    timeValue, AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, listener,
                    (actionListener) -> {
                        progressActionListener.searchProgressActionListener().removeListener(actionListener);
                        listener.onResponse(context.getAsyncSearchResponse());
                    });
            progressActionListener.searchProgressActionListener().addOrExecuteListener(wrappedListener);
        } else {
            // we don't need to wait any further on search progress
            logger.debug("Context is not running for async search id [{}]", context.getAsyncSearchId());
            listener.onResponse(context.getAsyncSearchResponse());
        }
    }
}
