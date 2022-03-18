/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.request;

import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.ValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

import static org.hamcrest.Matchers.containsString;

public class SubmitAsynchronousSearchRequestTests extends OpenSearchTestCase {

    public void testValidRequest() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        ValidationException validationException = request.validate();
        assertNull(validationException);
        assertEquals(request, new SubmitAsynchronousSearchRequest(searchRequest));
        String description = request.createTask(randomNonNegativeLong(), "taskType", SubmitAsynchronousSearchAction.NAME,
            new TaskId("test", -1), new HashMap<>()).getDescription();
        assertThat(description, containsString("indices[test]"));
    }

    public void testSuggestOnlyQueryFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder().suggest(new SuggestBuilder());
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
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
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertEquals("scrolls are not supported", validationException.validationErrors().get(0));
    }

    public void testCcsMinimizeRoundtripsFlagFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(
                searchRequest);
        searchRequest.setCcsMinimizeRoundtrips(true);
        ValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals(1, validationException.validationErrors().size());
        assertThat(validationException.validationErrors().get(0), containsString("[ccs_minimize_roundtrips] must be false"));
    }

    public void testInvalidKeepAliveFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.keepAlive(TimeValue.timeValueSeconds(59));
        ValidationException validationException = request.validate();
        assertEquals(1, validationException.validationErrors().size());
    }

    public void testInvalidWaitForCompletionFailsValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(-1));
        ValidationException validationException = request.validate();
        assertEquals(1, validationException.validationErrors().size());
    }
}
