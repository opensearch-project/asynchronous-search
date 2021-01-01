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

package com.amazon.opendistroforelasticsearch.search.async.task;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Task storing information about a currently running {@link SearchRequest}.
 */
public class AsyncSearchTask extends SearchTask {

    private static final Logger logger = LogManager.getLogger(AsyncSearchTask.class);

    private final Consumer<AsyncSearchActiveContext> freeActiveContextConsumer;
    private final AsyncSearchActiveContext asyncSearchActiveContext;
    private final SubmitAsyncSearchRequest request;

    public static final String NAME = "indices:data/read/opendistro/asynchronous_search";

    public AsyncSearchTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                           AsyncSearchActiveContext asyncSearchContext, SubmitAsyncSearchRequest request,
                           Consumer<AsyncSearchActiveContext> freeActiveContextConsumer) {
        super(id, type, action, null, parentTaskId, headers);
        Objects.requireNonNull(asyncSearchContext);
        Objects.requireNonNull(freeActiveContextConsumer);
        this.freeActiveContextConsumer = freeActiveContextConsumer;
        this.asyncSearchActiveContext = asyncSearchContext;
        this.request = request;
    }

    @Override
    protected void onCancelled() {
        logger.debug("On Cancelled event received for async search context [{}] due to [{}]", asyncSearchActiveContext.getAsyncSearchId(),
                getReasonCancelled());
        freeActiveContextConsumer.accept(asyncSearchActiveContext);
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder("[async search] :");
        sb.append("indices[");
        Strings.arrayToDelimitedString(request.getSearchRequest().indices(), ",", sb);
        sb.append("], ");
        sb.append("types[");
        Strings.arrayToDelimitedString(request.getSearchRequest().types(), ",", sb);
        sb.append("], ");
        sb.append("search_type[").append(request.getSearchRequest().searchType()).append("], ");
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
