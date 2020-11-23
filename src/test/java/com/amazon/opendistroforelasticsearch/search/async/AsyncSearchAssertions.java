package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

import static org.junit.Assert.assertEquals;

public class AsyncSearchAssertions {

    public static void assertSearchResponses(SearchResponse expected, SearchResponse actual) {
        assertEquals(expected.getSuccessfulShards(), actual.getSuccessfulShards());
        assertEquals(expected.getNumReducePhases(), actual.getNumReducePhases());
        assertEquals(expected.getClusters(), actual.getClusters());
        assertEquals(expected.getSkippedShards(), actual.getSkippedShards());
        assertEquals(expected.getTotalShards(), actual.getTotalShards());
        assertEquals(expected.getSuccessfulShards(), actual.getSuccessfulShards());
        assertEquals(expected.getAggregations(), actual.getAggregations());
        assertEquals(expected.getHits().getTotalHits(), actual.getHits().getTotalHits());
        ShardSearchFailure[] shardSearchFailures = expected.getShardFailures();
        if (shardSearchFailures != null && shardSearchFailures.length > 0) {
            assertFailures(actual);
            assertEquals(expected.getShardFailures().length, actual.getShardFailures().length);
            for (int i = 0; i < expected.getShardFailures().length; i++) {
                ShardSearchFailure originalFailure = actual.getShardFailures()[i];
                assertEquals(originalFailure.index(), actual.getShardFailures()[i].index());
                assertEquals(originalFailure.shard(), actual.getShardFailures()[i].shard());
                assertEquals(originalFailure.shardId(), actual.getShardFailures()[i].shardId());
                String originalMsg = originalFailure.getCause().getMessage();
                assertEquals(actual.getShardFailures()[i].getCause().getMessage(), originalMsg);
            }
        }
        else {
            assertNoFailures(actual);
        }
    }
}
