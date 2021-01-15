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

package com.amazon.opendistroforelasticsearch.search.asynchronous.restIT;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ApiParamsValidationIT extends AsynchronousSearchRestTestCase {

    public void testSubmitInvalidKeepAlive() throws IOException {
        try {
            SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest());
            request.keepAlive(TimeValue.timeValueDays(100));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(request));
            assertThat(responseException.getMessage(), containsString("Keep alive for asynchronous search (" +
                    request.getKeepAlive().getMillis() + ") is too large"));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitInvalidWaitForCompletion() throws IOException {
        try {
            SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest());
            request.waitForCompletionTimeout(TimeValue.timeValueMinutes(2));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(request));
            assertThat(responseException.getMessage(), containsString("Wait for completion timeout for asynchronous search (" +
                    request.getWaitForCompletionTimeout().getMillis() + ") is too large"));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitDefaultKeepAlive() throws IOException {
        try {
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                    new SearchRequest("test"));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            assertTrue((submitResponse.getExpirationTimeMillis()
                    > System.currentTimeMillis() + TimeValue.timeValueHours(23).getMillis() + TimeValue.timeValueMinutes(59).millis())   &&
                    (submitResponse.getExpirationTimeMillis() < System.currentTimeMillis() + +TimeValue.timeValueHours(24).getMillis()));
            assertHitCount(submitResponse.getSearchResponse(), 5);
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitDefaultWaitForCompletion() throws IOException {
        try {
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                    new SearchRequest("test"));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
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
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
            submitAsynchronousSearchRequest.keepOnCompletion(false);
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            assertHitCount(submitResponse.getSearchResponse(), 6);
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitSearchOnInvalidIndex() throws IOException {
        try {
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                    new SearchRequest("invalid-index"));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.FAILED, AsynchronousSearchState.CLOSED);
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
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
            getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueDays(100));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsynchronousSearch(
                    getAsynchronousSearchRequest));
            assertThat(responseException.getMessage(), containsString("Keep alive for asynchronous search (" +
                    getAsynchronousSearchRequest.getKeepAlive().getMillis() + ") is too large"));
            executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSuggestOnlySearchRequest() throws IOException {
        try {
            SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
            SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest(new String[]{"test"}, source));
            try {
                AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(request);
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
            SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
            try {
                AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(request);
            } catch (Exception e) {
                assertTrue(e instanceof ResponseException);
                assertThat(e.getMessage(), containsString("scrolls are not supported"));
            }
        } finally {
            deleteIndexIfExists();
        }
    }
}
