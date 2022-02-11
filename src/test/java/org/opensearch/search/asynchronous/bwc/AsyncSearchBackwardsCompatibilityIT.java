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

package org.opensearch.search.asynchronous.bwc;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.restIT.AsynchronousSearchRestTestCase;
import org.opensearch.search.asynchronous.utils.RestTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AsyncSearchBackwardsCompatibilityIT  extends AsynchronousSearchRestTestCase {
    public void testSubmitWithRetainedResponse() throws IOException {
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
    }


    AsynchronousSearchResponse executeSubmitAsynchronousSearch(@Nullable SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest)
            throws IOException {
        Request request = RestTestUtils.buildHttpRequest(submitAsynchronousSearchRequest);
        Response resp = client().performRequest(request);
        return parseEntity(resp.getEntity(), AsynchronousSearchResponse::fromXContent);
    }

    Response executeDeleteAsynchronousSearch(DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest) throws IOException {
        Request request = RestTestUtils.buildHttpRequest(deleteAsynchronousSearchRequest);
        return client().performRequest(request);
    }
}
