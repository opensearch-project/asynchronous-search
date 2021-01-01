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

package com.amazon.opendistroforelasticsearch.search.async.utils;

import org.elasticsearch.action.search.SearchResponse;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

import static org.junit.Assert.assertEquals;

public class AsyncSearchAssertions {

    public static void assertSearchResponses(SearchResponse expected, SearchResponse actual) {
        assertEquals(expected.getNumReducePhases(), actual.getNumReducePhases());
        assertEquals(expected.getClusters(), actual.getClusters());
        assertEquals(expected.getSkippedShards(), actual.getSkippedShards());
        assertEquals(expected.getTotalShards(), actual.getTotalShards());
        assertEquals(expected.getSuccessfulShards(), actual.getSuccessfulShards());
        assertEquals(expected.getAggregations(), actual.getAggregations());
        assertEquals(expected.getHits().getTotalHits(), actual.getHits().getTotalHits());
        assertNoFailures(actual);
    }
}
