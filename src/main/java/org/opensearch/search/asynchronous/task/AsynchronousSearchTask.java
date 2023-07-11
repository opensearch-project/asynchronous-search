/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.task;

import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.core.common.Strings;
import org.opensearch.tasks.TaskId;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Task storing information about a currently running {@link SearchRequest}.
 */
public class AsynchronousSearchTask extends SearchTask {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchTask.class);

    private final Consumer<AsynchronousSearchActiveContext> freeActiveContextConsumer;
    private final AsynchronousSearchActiveContext asynchronousSearchActiveContext;
    private final SubmitAsynchronousSearchRequest request;

    public static final String NAME = "indices:data/read/opendistro/asynchronous_search";

    public AsynchronousSearchTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                           AsynchronousSearchActiveContext asynchronousSearchContext, SubmitAsynchronousSearchRequest request,
                           Consumer<AsynchronousSearchActiveContext> freeActiveContextConsumer) {
        super(id, type, action, () -> description(request), parentTaskId, headers);
        Objects.requireNonNull(asynchronousSearchContext);
        Objects.requireNonNull(freeActiveContextConsumer);
        this.freeActiveContextConsumer = freeActiveContextConsumer;
        this.asynchronousSearchActiveContext = asynchronousSearchContext;
        this.request = request;
    }

    @Override
    protected void onCancelled() {
        logger.debug("On Cancelled event received for asynchronous search context [{}] due to [{}]",
                asynchronousSearchActiveContext.getAsynchronousSearchId(), getReasonCancelled());
        freeActiveContextConsumer.accept(asynchronousSearchActiveContext);
    }

    private static String description(SubmitAsynchronousSearchRequest request) {
        StringBuilder sb = new StringBuilder("[asynchronous search] :");
        sb.append("indices[");
        Strings.arrayToDelimitedString(request.getSearchRequest().indices(), ",", sb);
        sb.append("], ");
        sb.append("search_type[").append(request.getSearchRequest().searchType()).append("], ");
        sb.append("keep_on_completion[").append(request.getKeepOnCompletion()).append("], ");
        sb.append("keep_alive[").append(request.getKeepAlive()).append("], ");
        if (request.getSearchRequest().source() != null) {
            sb.append("source[").append(request.getSearchRequest().source()
                    .toString(SearchRequest.FORMAT_PARAMS)).append("]");
        } else {
            sb.append("source[]");
        }
        return sb.toString();
    }
}
