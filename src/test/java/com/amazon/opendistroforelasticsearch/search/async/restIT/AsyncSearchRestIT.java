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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class AsyncSearchRestIT extends AsyncSearchRestTestCase {

    public void testSubmitWithoutRetainedResponse() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(false);
            submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(
                    AsyncSearchState.RUNNING, AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            AsyncSearchResponse getResponse;
            do {
                getResponse = null;
                try {
                    getResponse = getAssertedAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
                    if (AsyncSearchState.SUCCEEDED.equals(getResponse.getState())
                            || AsyncSearchState.CLOSED.equals(getResponse.getState())) {
                        assertNotNull(getResponse.getSearchResponse());
                        assertHitCount(getResponse.getSearchResponse(), 5L);
                    }
                } catch (Exception e) {
                    assertRnf(e);
                }
            } while (getResponse != null && legalStates.contains(getResponse.getState()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitWithRetainedResponse() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(
                    AsyncSearchState.RUNNING, AsyncSearchState.SUCCEEDED, AsyncSearchState.PERSIST_SUCCEEDED, AsyncSearchState.PERSISTING,
                    AsyncSearchState.CLOSED, AsyncSearchState.STORE_RESIDENT);
            assertNotNull(submitResponse.getId());
            assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            AsyncSearchResponse getResponse;
            do {
                getResponse = getAssertedAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
                if (getResponse.getState() == AsyncSearchState.RUNNING && getResponse.getSearchResponse() != null) {
                    assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 0);
                } else {
                    assertNotNull(getResponse.getSearchResponse());
                    assertNotEquals(getResponse.getSearchResponse().getTook(), -1L);
                }
            } while (AsyncSearchState.STORE_RESIDENT.equals(getResponse.getState()) == false);
            getResponse = getAssertedAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertNotNull(getResponse.getSearchResponse());
            assertEquals(AsyncSearchState.STORE_RESIDENT, getResponse.getState());
            assertHitCount(getResponse.getSearchResponse(), 5);
            executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitSearchCompletesBeforeWaitForCompletionTimeout() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(TimeValue.timeValueHours(5));
            submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(1));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.PERSIST_SUCCEEDED,
                    AsyncSearchState.PERSISTING, AsyncSearchState.CLOSED, AsyncSearchState.STORE_RESIDENT);
            assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
            assertHitCount(submitResponse.getSearchResponse(), 5L);
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            AsyncSearchResponse getResponse = getAssertedAsyncSearchResponse(submitResponse, getAsyncSearchRequest);
            assertEquals(getResponse, submitResponse);
            executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testGetWithoutKeepAliveUpdate() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            AsyncSearchResponse getResponse = executeGetAsyncSearch(new GetAsyncSearchRequest(submitResponse.getId()));
            assertEquals(getResponse.getExpirationTimeMillis(), submitResponse.getExpirationTimeMillis());
            executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsyncSearch(
                    new GetAsyncSearchRequest(submitResponse.getId())));
            assertRnf(responseException);
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testGetWithKeepAliveUpdate() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            TimeValue keepAlive = TimeValue.timeValueHours(5);
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.keepAlive(keepAlive);
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
            GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
            getAsyncSearchRequest.setKeepAlive(keepAlive);
            AsyncSearchResponse getResponse = executeGetAsyncSearch(getAsyncSearchRequest);
            assertThat(getResponse.getExpirationTimeMillis(), greaterThan(submitResponse.getExpirationTimeMillis()));
            executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsyncSearch(
                    new GetAsyncSearchRequest(submitResponse.getId())));
            assertRnf(responseException);
        } finally {
            deleteIndexIfExists();
        }
    }
}
