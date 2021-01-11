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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class AsynchronousSearchRestIT extends AsynchronousSearchRestTestCase {

    public void testSubmitWithoutRetainedResponse() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(false);
            submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(
                    AsynchronousSearchState.RUNNING, AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
            assertTrue(legalStates.contains(submitResponse.getState()));
            GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
            AsynchronousSearchResponse getResponse;
            do {
                getResponse = null;
                try {
                    getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest);
                    if (AsynchronousSearchState.SUCCEEDED.equals(getResponse.getState())
                            || AsynchronousSearchState.CLOSED.equals(getResponse.getState())) {
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
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(
                    AsynchronousSearchState.RUNNING, AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.PERSIST_SUCCEEDED,
                    AsynchronousSearchState.PERSISTING,
                    AsynchronousSearchState.CLOSED, AsynchronousSearchState.STORE_RESIDENT);
            assertNotNull(submitResponse.getId());
            assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
            GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
            AsynchronousSearchResponse getResponse;
            do {
                getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest);
                if (getResponse.getState() == AsynchronousSearchState.RUNNING && getResponse.getSearchResponse() != null) {
                    assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 0);
                } else {
                    assertNotNull(getResponse.getSearchResponse());
                    assertNotEquals(getResponse.getSearchResponse().getTook(), -1L);
                }
            } while (AsynchronousSearchState.STORE_RESIDENT.equals(getResponse.getState()) == false);
            getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest);
            assertNotNull(getResponse.getSearchResponse());
            assertEquals(AsynchronousSearchState.STORE_RESIDENT, getResponse.getState());
            assertHitCount(getResponse.getSearchResponse(), 5);
            executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitSearchCompletesBeforeWaitForCompletionTimeout() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(TimeValue.timeValueHours(5));
            submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(1));
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED,
                    AsynchronousSearchState.PERSIST_SUCCEEDED,
                    AsynchronousSearchState.PERSISTING, AsynchronousSearchState.CLOSED, AsynchronousSearchState.STORE_RESIDENT);
            assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
            assertHitCount(submitResponse.getSearchResponse(), 5L);
            GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
            AsynchronousSearchResponse getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest);
            assertEquals(getResponse, submitResponse);
            executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testGetWithoutKeepAliveUpdate() throws IOException {
        try {
            SearchRequest searchRequest = new SearchRequest("test");
            searchRequest.source(new SearchSourceBuilder());
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            AsynchronousSearchResponse getResponse = executeGetAsynchronousSearch(new GetAsynchronousSearchRequest(submitResponse.getId()));
            assertEquals(getResponse.getExpirationTimeMillis(), submitResponse.getExpirationTimeMillis());
            executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsynchronousSearch(
                    new GetAsynchronousSearchRequest(submitResponse.getId())));
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
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
            submitAsynchronousSearchRequest.keepOnCompletion(true);
            submitAsynchronousSearchRequest.keepAlive(keepAlive);
            AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
            GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
            getAsynchronousSearchRequest.setKeepAlive(keepAlive);
            AsynchronousSearchResponse getResponse = executeGetAsynchronousSearch(getAsynchronousSearchRequest);
            assertThat(getResponse.getExpirationTimeMillis(), greaterThan(submitResponse.getExpirationTimeMillis()));
            executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsynchronousSearch(
                    new GetAsynchronousSearchRequest(submitResponse.getId())));
            assertRnf(responseException);
        } finally {
            deleteIndexIfExists();
        }
    }
}
