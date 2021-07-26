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

package org.opensearch.search.asynchronous.restIT;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.ResponseException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.suggest.SuggestBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ApiParamsValidationIT extends AsynchronousSearchRestTestCase {

    public void testSubmitInvalidKeepAlive() throws IOException {
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest());
        request.keepAlive(TimeValue.timeValueDays(100));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(request));
        assertThat(responseException.getMessage(), containsString("Keep alive for asynchronous search (" +
                request.getKeepAlive().getMillis() + ") is too large"));
    }

    public void testSubmitInvalidWaitForCompletion() throws IOException {
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest());
        request.waitForCompletionTimeout(TimeValue.timeValueMinutes(2));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(request));
        assertThat(responseException.getMessage(), containsString("Wait for completion timeout for asynchronous search (" +
                request.getWaitForCompletionTimeout().getMillis() + ") is too large"));
    }

    public void testSubmitDefaultKeepAlive() throws IOException {
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                new SearchRequest("test"));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
        List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
        assertTrue(legalStates.contains(submitResponse.getState()));
        assertTrue((submitResponse.getExpirationTimeMillis()
                > System.currentTimeMillis() + TimeValue.timeValueHours(23).getMillis() + TimeValue.timeValueMinutes(59).millis()) &&
                (submitResponse.getExpirationTimeMillis() < System.currentTimeMillis() + +TimeValue.timeValueHours(24).getMillis()));
        assertHitCount(submitResponse.getSearchResponse(), 5);
    }

    public void testSubmitDefaultWaitForCompletion() throws IOException {
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                new SearchRequest("test"));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
        List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.CLOSED);
        assertTrue(legalStates.contains(submitResponse.getState()));
        assertHitCount(submitResponse.getSearchResponse(), 5);
    }

    public void testSubmitSearchOnInvalidIndex() throws IOException {
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(
                new SearchRequest("invalid-index"));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest);
        List<AsynchronousSearchState> legalStates = Arrays.asList(AsynchronousSearchState.FAILED, AsynchronousSearchState.CLOSED);
        assertNull(submitResponse.getSearchResponse());
        assertNotNull(submitResponse.getError());
        assertThat(submitResponse.getError().getDetailedMessage(), containsString("index_not_found"));
        assertTrue(legalStates.contains(submitResponse.getState()));
    }

    public void testGetWithInvalidKeepAliveUpdate() throws IOException {
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
    }

    public void testSuggestOnlySearchRequest() {
        SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest(new String[]{"test"}, source));
        try {
            AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(request);
        } catch (Exception e) {
            assertTrue(e instanceof ResponseException);
            assertThat(e.getMessage(), containsString("suggest-only queries are not supported"));
        }

    }

    public void testScrollSearchRequest() {
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.scroll(TimeValue.timeValueMinutes(1));
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        try {
            AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(request);
        } catch (Exception e) {
            assertTrue(e instanceof ResponseException);
            assertThat(e.getMessage(), containsString("scrolls are not supported"));
        }
    }
}
