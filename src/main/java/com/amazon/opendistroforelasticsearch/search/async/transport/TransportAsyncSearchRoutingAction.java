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

import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.AsyncSearchRoutingRequest;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import com.amazon.opendistroforelasticsearch.search.async.utils.AsyncSearchExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

/**
 * Base class for the action to be executed on the coordinator running the async search from the initial
 * {@link TransportSubmitAsyncSearchAction}. The class forwards the request to the coordinator and executes the
 * {@link TransportGetAsyncSearchAction} or the {@link TransportDeleteAsyncSearchAction}
 */
public abstract class TransportAsyncSearchRoutingAction<Request extends AsyncSearchRoutingRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportAsyncSearchRoutingAction.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Writeable.Reader<Response> responseReader;
    private final String actionName;
    private final ThreadPool threadPool;
    private final Client client;
    private final AsyncSearchService asyncSearchService;

    public TransportAsyncSearchRoutingAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                             Client client, String actionName, ActionFilters actionFilters,
                                             AsyncSearchService asyncSearchService, Writeable.Reader<Request> requestReader,
                                             Writeable.Reader<Response> responseReader) {
        super(actionName, transportService, actionFilters, requestReader);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.responseReader = responseReader;
        this.actionName = actionName;
        this.threadPool = threadPool;
        this.client = client;
        this.asyncSearchService = asyncSearchService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            new AsyncForwardAction(request, listener).run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public abstract void handleRequest(AsyncSearchId asyncSearchId, Request request, ActionListener<Response> listener, User user);

    final class AsyncForwardAction extends AbstractRunnable {

        private final ActionListener<Response> listener;
        private final Request request;
        private DiscoveryNode targetNode;
        private AsyncSearchId asyncSearchId;

        AsyncForwardAction(Request request, ActionListener<Response> listener) {
            try {
                this.asyncSearchId = AsyncSearchIdConverter.parseAsyncId(request.getId());

                this.request = request;
                this.listener = listener;
                this.targetNode = clusterService.state().nodes().get(asyncSearchId.getNode());
            } catch (IllegalArgumentException e) { // failure in parsing async search
                logger.error(() -> new ParameterizedMessage("Failed to parse async search ID [{}]", request.getId()), e);
                listener.onFailure(AsyncSearchExceptionUtils.buildResourceNotFoundException(request.getId()));
                throw e;
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to dispatch request for action [{}] for async search [{}]", actionName,
                    request.getId()), e);
            sendLocalRequest(asyncSearchId, request, listener);
        }

        @Override
        protected void doRun() {
            ClusterState state = clusterService.state();
            // forward request only if the local node isn't the node coordinating the search and the node coordinating
            // the search exists in the cluster
            TransportRequestOptions requestOptions = TransportRequestOptions.builder().withTimeout(
                    asyncSearchService.getMaxWaitForCompletionTimeout()).build();
            if (targetNode != null && state.nodes().getLocalNode().equals(targetNode) == false && state.nodes().nodeExists(targetNode)) {
                logger.debug("Forwarding async search id [{}] request to target node [{}]", request.getId(), targetNode);
                transportService.sendRequest(targetNode, actionName, request, requestOptions,
                        new ActionListenerResponseHandler<Response>(listener, responseReader) {
                            @Override
                            public void handleException(final TransportException exp) {
                                Throwable cause = exp.unwrapCause();
                                if (cause instanceof ConnectTransportException ||
                                        (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                    // we want to retry here a bit to see if the node connects backs
                                    logger.debug("Connection exception while trying to forward request with id[{}] to " +
                                                    "target node [{}] Error: [{}]",
                                            request.getId(), targetNode, exp.getDetailedMessage());
                                    //try on local node since we weren't able to forward
                                    sendLocalRequest(asyncSearchId, request, listener);
                                } else {
                                    logger.debug("Exception received for request with id[{}] to from target node [{}],  Error: [{}]",
                                            request.getId(), targetNode, exp.getDetailedMessage());
                                    listener.onFailure(cause instanceof Exception ? (Exception) cause
                                            : new NotSerializableExceptionWrapper(cause));
                                }
                            }

                            @Override
                            public void handleResponse(Response response) {
                                logger.debug("Received the response for async search id [{}] from target node [{}]", request.getId(),
                                        targetNode);
                                listener.onResponse(response);
                            }
                        });
            } else {
                sendLocalRequest(asyncSearchId, request, listener);
            }
        }

        private void sendLocalRequest(AsyncSearchId asyncSearchId, Request request, ActionListener<Response> listener) {
            ThreadContext threadContext = threadPool.getThreadContext();
            String userStr = threadContext.getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT);
            User user = User.parse(userStr);
            try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
                handleRequest(asyncSearchId, request, listener, user);
            }
        }
    }
}
