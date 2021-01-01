package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class SubmitAsyncSearchRequestTests extends ESTestCase {

    public void testValidRequest() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsyncSearchRequest request = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
        ValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testSuggestOnlyQueryFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsyncSearchRequest request = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("suggest-only queries are not supported", validationException.validationErrors().get(0));
    }

    public void testSearchScrollFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        searchRequest.scroll(randomTimeValue());
        SubmitAsyncSearchRequest request = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("scrolls are not supported", validationException.validationErrors().get(0));
    }

    public void testCcsMinimizeRoundtripsFlagFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(
                searchRequest);
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertThat(validationException.validationErrors().get(0), containsString("[ccs_minimize_roundtrips] must be false"));
    }
}
