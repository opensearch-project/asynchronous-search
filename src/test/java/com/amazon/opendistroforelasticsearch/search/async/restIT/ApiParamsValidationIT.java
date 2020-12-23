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

import static org.hamcrest.Matchers.containsString;

public class ApiParamsValidationIT extends AsyncSearchRestTestCase {

    public void testSubmitInvalidKeepAlive() {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest());
        request.keepAlive(TimeValue.timeValueDays(100));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(request));
        assertThat(responseException.getMessage(), containsString("Keep alive for async search (" +
                request.getKeepAlive().getMillis() + ") is too large"));
    }

    public void testSubmitInvalidWaitForCompletion() {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest());
        request.waitForCompletionTimeout(TimeValue.timeValueMinutes(2));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(request));
        assertThat(responseException.getMessage(), containsString("Wait for completion timeout for async search (" +
                request.getWaitForCompletionTimeout().getMillis() + ") is too large"));
    }

    public void testSubmitDefaultKeepAlive() throws IOException {
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("test"));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
        assertTrue(legalStates.contains(submitResponse.getState()));
        assertTrue((submitResponse.getExpirationTimeMillis() > System.currentTimeMillis() + TimeValue.timeValueDays(4).getMillis()) &&
                (submitResponse.getExpirationTimeMillis() < System.currentTimeMillis() + +TimeValue.timeValueDays(5).getMillis()));
        assertHitCount(submitResponse.getSearchResponse(), 5);
    }

    public void testSubmitDefaultWaitForCompletion() throws IOException {
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("test"));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
        assertTrue(legalStates.contains(submitResponse.getState()));
        assertHitCount(submitResponse.getSearchResponse(), 5);
    }

    /**
     * run search on all indices
     */
    public void testSubmitSearchAllIndices() throws IOException {
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest());
        submitAsyncSearchRequest.keepOnCompletion(false);
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.SUCCEEDED, AsyncSearchState.CLOSED);
        assertTrue(legalStates.contains(submitResponse.getState()));
        assertHitCount(submitResponse.getSearchResponse(), 6);
    }

    public void testSubmitSearchOnInvalidIndex() throws IOException {
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(new SearchRequest("afknwefwoef"));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        List<AsyncSearchState> legalStates = Arrays.asList(AsyncSearchState.FAILED, AsyncSearchState.CLOSED);
        assertNull(submitResponse.getSearchResponse());
        assertNotNull(submitResponse.getError());
        assertThat(submitResponse.getError().getDetailedMessage(), containsString("index_not_found"));
        assertTrue(legalStates.contains(submitResponse.getState()));
    }

    public void testGetWithInvalidKeepAliveUpdate() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test");
        TimeValue keepAlive = TimeValue.timeValueDays(5);
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.keepAlive(keepAlive);
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(submitAsyncSearchRequest);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueDays(100));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeGetAsyncSearch(getAsyncSearchRequest));
        assertThat(responseException.getMessage(), containsString("Keep alive for async search (" +
                getAsyncSearchRequest.getKeepAlive().getMillis() + ") is too large"));
        executeDeleteAsyncSearch(new DeleteAsyncSearchRequest(submitResponse.getId()));

    }

}
