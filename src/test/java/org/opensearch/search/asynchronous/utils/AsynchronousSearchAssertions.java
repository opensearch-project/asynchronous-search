/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.utils;

import org.opensearch.action.search.SearchResponse;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

import static org.junit.Assert.assertEquals;

public class AsynchronousSearchAssertions {

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
