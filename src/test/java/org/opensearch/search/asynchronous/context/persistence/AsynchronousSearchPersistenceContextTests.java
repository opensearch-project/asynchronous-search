/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.persistence;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.utils.TestClientUtils;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

public class AsynchronousSearchPersistenceContextTests extends OpenSearchTestCase {

    /**
     * asynchronous search persistence context serializes search response into {@linkplain BytesReference}. We verify that de-serializing
     * the{@linkplain BytesReference} yields the same object.@throws IOException when there is a serialization issue
     */
    public void testSerializationRoundTripWithSearchResponse() throws IOException {
        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
        String id = AsynchronousSearchIdConverter.buildAsyncId(new AsynchronousSearchId(UUID.randomUUID().toString(),
                randomNonNegativeLong(), asContextId));
        long expirationTimeMillis = randomNonNegativeLong();
        long startTimeMillis = randomNonNegativeLong();
        SearchResponse searchResponse = getMockSearchResponse();
        User user = TestClientUtils.randomUserOrNull();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        AsynchronousSearchPersistenceContext asPersistenceContext =
                new AsynchronousSearchPersistenceContext(id, asContextId, new AsynchronousSearchPersistenceModel(startTimeMillis,
                        expirationTimeMillis, searchResponse, null, user), System::currentTimeMillis,
                        new NamedWriteableRegistry(searchModule.getNamedWriteables()));
        assertEquals(asPersistenceContext, new AsynchronousSearchPersistenceContext(id, asContextId,
                new AsynchronousSearchPersistenceModel(startTimeMillis, expirationTimeMillis, searchResponse, null, user),
                System::currentTimeMillis, new NamedWriteableRegistry(Collections.emptyList())));
        assertEquals(
                asPersistenceContext.getAsynchronousSearchResponse(),
                new AsynchronousSearchResponse(id, asPersistenceContext.getAsynchronousSearchState(), startTimeMillis,
                        expirationTimeMillis, searchResponse, null));
    }

    /**
     * asynchronous search persistence model serializes exception into {@linkplain BytesReference}. We verify that de-serializing the
     * {@linkplain BytesReference} yields the same object.
     *
     * @throws IOException when there is a serialization issue
     */
    public void testSerializationRoundTripWithError() throws IOException {
        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
        String id = AsynchronousSearchIdConverter.buildAsyncId(new AsynchronousSearchId(UUID.randomUUID().toString(),
                randomNonNegativeLong(),
                asContextId));
        long expirationTimeMillis = randomNonNegativeLong();
        long startTimeMillis = randomNonNegativeLong();
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new RuntimeException("runtime-exception"));
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException("phase", "msg", new NullPointerException(),
                new ShardSearchFailure[] {shardSearchFailure});
        User user = TestClientUtils.randomUser();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        AsynchronousSearchPersistenceContext asPersistenceContext = new AsynchronousSearchPersistenceContext(id, asContextId,
                new AsynchronousSearchPersistenceModel(startTimeMillis, expirationTimeMillis, null, exception, user),
                System::currentTimeMillis,
                new NamedWriteableRegistry(searchModule.getNamedWriteables()));
        OpenSearchException deserializedException = asPersistenceContext.getAsynchronousSearchResponse().getError();
        assertTrue(deserializedException instanceof SearchPhaseExecutionException);
        assertEquals("phase", ((SearchPhaseExecutionException) deserializedException).getPhaseName());
        assertEquals("msg", deserializedException.getMessage());
        assertTrue(deserializedException.getCause() instanceof NullPointerException);
        assertEquals(1, ((SearchPhaseExecutionException) deserializedException).shardFailures().length);
        assertTrue(((SearchPhaseExecutionException) deserializedException).shardFailures()[0].getCause() instanceof RuntimeException);
    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

