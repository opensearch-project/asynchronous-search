package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

public class SubmitAsyncSearchRequestTests extends ESTestCase {

    public void testValidRequest() {

        SearchSourceBuilder source = new SearchSourceBuilder();
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(new String[]{"test"}, source));
        ValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testSuggestOnlyQueryFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(new String[]{"test"}, source));
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("suggest-only queries are not supported", validationException.validationErrors().get(0));
    }

    public void testSearchScrollFailsValidation() {
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
            searchRequest.scroll(randomTimeValue());
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
            ValidationException validationException = request.validate();
            assertNotNull(validationException);
            assertEquals(1, validationException.validationErrors().size());
            assertEquals("scrolls are not supported", validationException.validationErrors().get(0));
        }
    }


}
