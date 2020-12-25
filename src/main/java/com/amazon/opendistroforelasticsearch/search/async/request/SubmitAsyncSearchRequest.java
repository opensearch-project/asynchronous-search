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

package com.amazon.opendistroforelasticsearch.search.async.request;

import com.amazon.opendistroforelasticsearch.search.async.task.SubmitAsyncSearchTask;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SubmitAsyncSearchRequest extends ActionRequest {

    public static long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();
    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 1;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 5;
    public static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = TimeValue.timeValueSeconds(1);
    public static final Boolean DEFAULT_KEEP_ON_COMPLETION = Boolean.FALSE;
    public static final Boolean CCR_MINIMIZE_ROUNDTRIPS = Boolean.FALSE;
    public static final Boolean DEFAULT_REQUEST_CACHE = Boolean.TRUE;


    /**
     * The minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    @Nullable
    private TimeValue waitForCompletionTimeout = DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT;

    /**
     * Determines whether the resource resource should be kept on completion or failure (defaults to false).
     */
    @Nullable
    private Boolean keepOnCompletion = DEFAULT_KEEP_ON_COMPLETION;

    /**
     * The amount of time after which the result will expire
     */
    @Nullable
    private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;

    /**
     * The underlying search request to execute
     */
    private final SearchRequest searchRequest;


    /**
     * Creates a new request from a {@linkplain SearchRequest}
     *
     * @param searchRequest the search request
     */
    public SubmitAsyncSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        this.searchRequest.setCcsMinimizeRoundtrips(CCR_MINIMIZE_ROUNDTRIPS);
        this.searchRequest.setPreFilterShardSize(DEFAULT_PRE_FILTER_SHARD_SIZE); //TODO update rest action to add defaults if params missing
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public void waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    public Boolean getKeepOnCompletion() {
        return keepOnCompletion;
    }

    public void keepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    public SubmitAsyncSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.searchRequest = new SearchRequest(in);
        this.waitForCompletionTimeout = in.readOptionalTimeValue();
        this.keepAlive = in.readTimeValue();
        this.keepOnCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.searchRequest.writeTo(out);
        out.writeOptionalTimeValue(waitForCompletionTimeout);
        out.writeTimeValue(keepAlive);
        out.writeBoolean(keepOnCompletion);
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (searchRequest.isSuggestOnly()) {
            validationException = addValidationError("suggest-only queries are not supported", validationException);
        }
        if (searchRequest.scroll() != null) {
            validationException = addValidationError("scrolls are not supported", validationException);
        }
        if (searchRequest.isCcsMinimizeRoundtrips()) {
            validationException = addValidationError(
                    "[ccs_minimize_roundtrips] must be false, got: " + searchRequest.isCcsMinimizeRoundtrips(), validationException);
        }
        if (keepAlive != null && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException = addValidationError(
                    "[keep_alive] must be greater than 1 minute, got: " + keepAlive.toString(), validationException);
        }
        return validationException != null ? validationException : searchRequest.validate();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubmitAsyncSearchRequest request = (SubmitAsyncSearchRequest) o;
        return Objects.equals(searchRequest, request.searchRequest)
                && Objects.equals(keepAlive, request.getKeepAlive())
                && Objects.equals(waitForCompletionTimeout, request.getWaitForCompletionTimeout())
                && Objects.equals(keepOnCompletion, request.getKeepOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchRequest, keepAlive, waitForCompletionTimeout, keepOnCompletion);
    }

    @Override
    public SubmitAsyncSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // generating description in a lazy way since source can be quite big
        SubmitAsyncSearchTask submitAsyncSearchTask = new SubmitAsyncSearchTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                StringBuilder sb = new StringBuilder();
                sb.append("indices[");
                Strings.arrayToDelimitedString(searchRequest.indices(), ",", sb);
                sb.append("], ");
                sb.append("types[");
                Strings.arrayToDelimitedString(searchRequest.types(), ",", sb);
                sb.append("], ");
                sb.append("search_type[").append(searchRequest.types()).append("], ");
                if (searchRequest.source() != null) {
                    sb.append("source[").append(searchRequest.source().toString(SearchRequest.FORMAT_PARAMS)).append("]");
                } else {
                    sb.append("source[]");
                }
                return sb.toString();
            }
        };
        return submitAsyncSearchTask;
    }
}
