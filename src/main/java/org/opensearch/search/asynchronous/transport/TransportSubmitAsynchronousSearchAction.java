/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.transport;

import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchTimeoutWrapper;
import org.opensearch.search.asynchronous.listener.PrioritizedActionListener;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.opensearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Submits an asynchronous search request by executing a {@link TransportSearchAction} on an {@link AsynchronousSearchTask} with
 * a {@link AsynchronousSearchProgressListener} set on the task. The listener is wrapped with a completion timeout wrapper via
 * {@link AsynchronousSearchTimeoutWrapper} which ensures that exactly one of action listener or the timeout listener gets executed
 */
public class TransportSubmitAsynchronousSearchAction extends HandledTransportAction<
    SubmitAsynchronousSearchRequest,
    AsynchronousSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSubmitAsynchronousSearchAction.class);
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final AsynchronousSearchService asynchronousSearchService;
    private final SearchService searchService;

    @Inject
    public TransportSubmitAsynchronousSearchAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        AsynchronousSearchService asynchronousSearchService,
        TransportSearchAction transportSearchAction,
        SearchService searchService
    ) {
        super(SubmitAsynchronousSearchAction.NAME, transportService, actionFilters, SubmitAsynchronousSearchRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.asynchronousSearchService = asynchronousSearchService;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, SubmitAsynchronousSearchRequest request, ActionListener<AsynchronousSearchResponse> listener) {
        AsynchronousSearchContext asynchronousSearchContext = null;
        String userStr = threadPool.getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        User user = User.parse(userStr);
        try {
            final long relativeStartTimeInMillis = threadPool.relativeTimeInMillis();
            asynchronousSearchContext = asynchronousSearchService.createAndStoreContext(
                request,
                relativeStartTimeInMillis,
                () -> searchService.aggReduceContextBuilder(request.getSearchRequest().source()),
                user
            );
            assert asynchronousSearchContext.getAsynchronousSearchProgressListener() != null
                : "missing progress listener for an active context";
            AsynchronousSearchProgressListener progressListener = asynchronousSearchContext.getAsynchronousSearchProgressListener();
            AsynchronousSearchContext context = asynchronousSearchContext; // making it effectively final for usage in anonymous class.
            SearchRequest searchRequest = new SearchRequest(request.getSearchRequest()) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    AsynchronousSearchTask asynchronousSearchTask = new AsynchronousSearchTask(
                        id,
                        type,
                        AsynchronousSearchTask.NAME,
                        parentTaskId,
                        headers,
                        (AsynchronousSearchActiveContext) context,
                        request,
                        asynchronousSearchService::onCancelledFreeActiveContext
                    );

                    asynchronousSearchService.bootstrapSearch(asynchronousSearchTask, context.getContextId());
                    PrioritizedActionListener<AsynchronousSearchResponse> wrappedListener = AsynchronousSearchTimeoutWrapper
                        .wrapScheduledTimeout(
                            threadPool,
                            request.getWaitForCompletionTimeout(),
                            AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME,
                            listener,
                            (actionListener) -> {
                                progressListener.searchProgressActionListener().removeListener(actionListener);
                                listener.onResponse(context.getAsynchronousSearchResponse());
                            }
                        );
                    progressListener.searchProgressActionListener().addOrExecuteListener(wrappedListener);
                    return asynchronousSearchTask;
                }
            };
            // set the parent task as the submit task for cancellation on connection close
            searchRequest.setParentTask(task.taskInfo(clusterService.localNode().getId(), false).getTaskId());
            transportSearchAction.execute(searchRequest, progressListener);

        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to submit asynchronous search request [{}]", request), e);
            if (asynchronousSearchContext != null) {
                AsynchronousSearchActiveContext asynchronousSearchActiveContext =
                    (AsynchronousSearchActiveContext) asynchronousSearchContext;
                asynchronousSearchService.freeContext(
                    asynchronousSearchActiveContext.getAsynchronousSearchId(),
                    asynchronousSearchActiveContext.getContextId(),
                    user,
                    ActionListener.wrap((r) -> {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "Successfully cleaned up context on submit asynchronous" + " search [{}] on failure",
                                asynchronousSearchActiveContext.getAsynchronousSearchId()
                            ),
                            e
                        );
                        listener.onFailure(e);
                    }, (ex) -> {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "Failed to cleaned up context on submit asynchronous search" + " [{}] on failure",
                                asynchronousSearchActiveContext.getAsynchronousSearchId()
                            ),
                            ex
                        );
                        listener.onFailure(e);
                    })
                );
            } else {
                listener.onFailure(e);
            }
        }
    }
}
