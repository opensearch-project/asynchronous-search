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

package com.amazon.opendistroforelasticsearch.search.async.restIT;

import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ApiParamsValidationIT extends AsyncSearchRestTestCase {

    public void testSubmitInvalidKeepAlive() throws IOException {
        try {
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest());
            request.keepAlive(TimeValue.timeValueDays(100));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(request));
            assertThat(responseException.getMessage(), containsString("Keep alive for async search (" +
                    request.getKeepAlive().getMillis() + ") is too large"));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitInvalidWaitForCompletion() throws IOException {
        try {
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest());
            request.waitForCompletionTimeout(TimeValue.timeValueMinutes(2));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(request));
            assertThat(responseException.getMessage(), containsString("Wait for completion timeout for async search (" +
                    request.getWaitForCompletionTimeout().getMillis() + ") is too large"));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitDefaultKeepAlive() throws IOException {
        try {
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("test"));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            assertTrue((submitResponse.getExpirationTimeMillis() > System.currentTimeMillis() + TimeValue.timeValueHours(11).getMillis()) &&
                    (submitResponse.getExpirationTimeMillis() < System.currentTimeMillis() + +TimeValue.timeValueHours(12).getMillis()));
            assertHitCount(submitResponse.getSearchResponse(), 5);
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitDefaultWaitForCompletion() throws IOException {
        try {
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("test"));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            assertHitCount(submitResponse.getSearchResponse(), 5);
        } finally {
            deleteIndexIfExists();
        }
    }

    /**
     * run search on all indices
     */
    public void testSubmitSearchAllIndices() throws IOException {
        try {
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            submitAsyncSearchRequest.keepOnCompletion(false);
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            assertHitCount(submitResponse.getSearchResponse(), 6);
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitSearchOnInvalidIndex() throws IOException {
        try {
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("afknwefwoef"));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.FAILED, AsyncSearchState.CLOSED);
            assertNull(submitResponse.getSearchResponse());
            assertNotNull(submitResponse.getError());
            assertThat(submitResponse.getError().getDetailedMessage(), containsString("index_not_found"));
            assertTrue(legalStates.contains(submitResponse.getState()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testGetWithInvalidKeepAliveUpdate() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueDays(100));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsyncSearch(getAsyncSearchRequest));
            assertThat(responseException.getMessage(), containsString("Keep alive for async search (" +
                    getAsyncSearchRequest.getKeepAlive().getMillis() + ") is too large"));
            executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSuggestOnlySearchRequest() throws IOException {
        try {
            SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(new String[]{"test"}, source));
            try {
                AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(request);
            } catch (Exception e) {
                assertTrue(e instanceof ResponseException);
                assertThat(e.getMessage(), containsString("suggest-only queries are not supported"));
            }
        } finally {
            deleteIndexIfExists();
        }

    }

    public void testScrollSearchRequest() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.scroll(TimeValue.timeValueMinutes(1));
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
            try {
                AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(request);
            } catch (Exception e) {
                assertTrue(e instanceof ResponseException);
                assertThat(e.getMessage(), containsString("scrolls are not supported"));
            }
        } finally {
            deleteIndexIfExists();
        }
    }
}
