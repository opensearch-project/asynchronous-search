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

package com.amazon.opendistroforelasticsearch.search.asynchronous.request;

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
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
        String description = request.createTask(randomNonNegativeLong(), "type", SubmitAsynchronousSearchAction.NAME,
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
