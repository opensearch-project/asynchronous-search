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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class SubmitAsynchronousSearchRequestTests extends ESTestCase {

    public void testValidRequest() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        SearchRequest searchRequest = new SearchRequest(new String[]{"test"}, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        ValidationException validationException = request.validate();
        assertNull(validationException);
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
}
