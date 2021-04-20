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

import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.AsynchronousSearchRoutingRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.AsynchronousSearchExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

/**
 * Base class for the action to be executed on the coordinator running the asynchronous search from the initial
 * {@link TransportSubmitAsynchronousSearchAction}. The class forwards the request to the coordinator and executes the
 * {@link TransportGetAsynchronousSearchAction} or the {@link TransportDeleteAsynchronousSearchAction}
 */
public abstract class TransportAsynchronousSearchRoutingAction<Request extends AsynchronousSearchRoutingRequest<Request>,
        Response extends ActionResponse> extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportAsynchronousSearchRoutingAction.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final Writeable.Reader<Response> responseReader;
    private final String actionName;
    private final ThreadPool threadPool;
    private final Client client;
    private final AsynchronousSearchService asynchronousSearchService;

    public TransportAsynchronousSearchRoutingAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                             Client client, String actionName, ActionFilters actionFilters,
                                             AsynchronousSearchService asynchronousSearchService, Writeable.Reader<Request> requestReader,
                                             Writeable.Reader<Response> responseReader) {
        super(actionName, transportService, actionFilters, requestReader);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.responseReader = responseReader;
        this.actionName = actionName;
        this.threadPool = threadPool;
        this.client = client;
        this.asynchronousSearchService = asynchronousSearchService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            new AsyncForwardAction(request, listener).run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public abstract void handleRequest(AsynchronousSearchId asynchronousSearchId, Request request, ActionListener<Response> listener,
                                       User user);

    final class AsyncForwardAction extends AbstractRunnable {

        private final ActionListener<Response> listener;
        private final Request request;
        private DiscoveryNode targetNode;
        private AsynchronousSearchId asynchronousSearchId;

        AsyncForwardAction(Request request, ActionListener<Response> listener) {
            try {
                this.asynchronousSearchId = AsynchronousSearchIdConverter.parseAsyncId(request.getId());

                this.request = request;
                this.listener = listener;
                this.targetNode = clusterService.state().nodes().get(asynchronousSearchId.getNode());
            } catch (IllegalArgumentException e) { // failure in parsing asynchronous search
                logger.error(() -> new ParameterizedMessage("Failed to parse asynchronous search ID [{}]", request.getId()), e);
                listener.onFailure(AsynchronousSearchExceptionUtils.buildResourceNotFoundException(request.getId()));
                throw e;
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage(
                    "Failed to dispatch request for action [{}] for asynchronous search [{}]", actionName, request.getId()), e);
            sendLocalRequest(asynchronousSearchId, request, listener);
        }

        @Override
        protected void doRun() {
            ClusterState state = clusterService.state();
            // forward request only if the local node isn't the node coordinating the search and the node coordinating
            // the search exists in the cluster
            TransportRequestOptions requestOptions = TransportRequestOptions.builder().withTimeout(
                    asynchronousSearchService.getMaxWaitForCompletionTimeout()).build();
            if (targetNode != null && state.nodes().getLocalNode().equals(targetNode) == false && state.nodes().nodeExists(targetNode)) {
                logger.debug("Forwarding asynchronous search id [{}] request to target node [{}]", request.getId(), targetNode);
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
                                    sendLocalRequest(asynchronousSearchId, request, listener);
                                } else {
                                    logger.debug("Exception received for request with id[{}] to from target node [{}],  Error: [{}]",
                                            request.getId(), targetNode, exp.getDetailedMessage());
                                    listener.onFailure(cause instanceof Exception ? (Exception) cause
                                            : new NotSerializableExceptionWrapper(cause));
                                }
                            }

                            @Override
                            public void handleResponse(Response response) {
                                logger.debug("Received the response for asynchronous search id [{}] from target node [{}]", request.getId(),
                                        targetNode);
                                listener.onResponse(response);
                            }
                        });
            } else {
                sendLocalRequest(asynchronousSearchId, request, listener);
            }
        }

        private void sendLocalRequest(AsynchronousSearchId asynchronousSearchId, Request request, ActionListener<Response> listener) {
            ThreadContext threadContext = threadPool.getThreadContext();
            String userStr = threadContext.getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT);
            User user = User.parse(userStr);
            try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
                handleRequest(asynchronousSearchId, request, listener, user);
            }
        }
    }
}
