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
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchTimeoutWrapper;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.PrioritizedActionListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Submits an asynchronous search request by executing a {@link TransportSearchAction} on an {@link AsynchronousSearchTask} with
 * a {@link AsynchronousSearchProgressListener} set on the task. The listener is wrapped with a completion timeout wrapper via
 * {@link AsynchronousSearchTimeoutWrapper} which ensures that exactly one of action listener or the timeout listener gets executed
 */
public class TransportSubmitAsynchronousSearchAction extends HandledTransportAction<SubmitAsynchronousSearchRequest,
        AsynchronousSearchResponse> {

    private static final Logger logger = LogManager.getLogger(TransportSubmitAsynchronousSearchAction.class);
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final AsynchronousSearchService asynchronousSearchService;
    private final SearchService searchService;

    @Inject
    public TransportSubmitAsynchronousSearchAction(ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            ActionFilters actionFilters, AsynchronousSearchService asynchronousSearchService,
                                            TransportSearchAction transportSearchAction, SearchService searchService) {
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
        String userStr = threadPool.getThreadContext().getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT);
        User user = User.parse(userStr);
        try {
            final long relativeStartTimeInMillis = threadPool.relativeTimeInMillis();
            asynchronousSearchContext = asynchronousSearchService.createAndStoreContext(request, relativeStartTimeInMillis,
                    () -> searchService.aggReduceContextBuilder(request.getSearchRequest()), user);
            assert asynchronousSearchContext.getAsynchronousSearchProgressListener() != null
                    : "missing progress listener for an active context";
            AsynchronousSearchProgressListener progressListener = asynchronousSearchContext.getAsynchronousSearchProgressListener();
            AsynchronousSearchContext context = asynchronousSearchContext; //making it effectively final for usage in anonymous class.
            SearchRequest searchRequest = new SearchRequest(request.getSearchRequest()) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    AsynchronousSearchTask asynchronousSearchTask = new AsynchronousSearchTask(id, type, AsynchronousSearchTask.NAME,
                            parentTaskId, headers,
                            (AsynchronousSearchActiveContext) context, request, asynchronousSearchService::onCancelledFreeActiveContext);

                    asynchronousSearchService.bootstrapSearch(asynchronousSearchTask, context.getContextId());
                    PrioritizedActionListener<AsynchronousSearchResponse> wrappedListener = AsynchronousSearchTimeoutWrapper
                            .wrapScheduledTimeout(threadPool, request.getWaitForCompletionTimeout(),
                                    AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, listener,
                                    (actionListener) -> { progressListener.searchProgressActionListener().removeListener(actionListener);
                                        listener.onResponse(context.getAsynchronousSearchResponse());
                                    });
                    progressListener.searchProgressActionListener().addOrExecuteListener(wrappedListener);
                    return asynchronousSearchTask;
                }
            };
            //set the parent task as the submit task for cancellation on connection close
            searchRequest.setParentTask(task.taskInfo(clusterService.localNode().getId(), false).getTaskId());
            transportSearchAction.execute(searchRequest, progressListener);

        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to submit asynchronous search request [{}]", request), e);
            if (asynchronousSearchContext != null) {
                AsynchronousSearchActiveContext asynchronousSearchActiveContext = (AsynchronousSearchActiveContext)
                        asynchronousSearchContext;
                asynchronousSearchService.freeContext(asynchronousSearchActiveContext.getAsynchronousSearchId(),
                        asynchronousSearchActiveContext.getContextId(), user,
                        ActionListener.wrap((r) -> {
                            logger.debug(() -> new ParameterizedMessage("Successfully cleaned up context on submit asynchronous" +
                                    " search [{}] on failure", asynchronousSearchActiveContext.getAsynchronousSearchId()), e);
                            listener.onFailure(e);
                        }, (ex) -> {
                            logger.debug(() -> new ParameterizedMessage("Failed to cleaned up context on submit asynchronous search" +
                                    " [{}] on failure", asynchronousSearchActiveContext.getAsynchronousSearchId()), ex);
                            listener.onFailure(e);
                        })
                );
            } else {
                listener.onFailure(e);
            }
        }
    }
}
