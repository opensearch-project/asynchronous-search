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

package com.amazon.opendistroforelasticsearch.search.asynchronous.response;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.Requests;
import org.opensearch.common.Randomness;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.opensearch.common.xcontent.XContentHelper.toXContent;

public class AsynchronousSearchResponseTests extends AbstractSerializingTestCase<AsynchronousSearchResponse> {

    @Override
    protected AsynchronousSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return AsynchronousSearchResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AsynchronousSearchResponse> instanceReader() {
        return AsynchronousSearchResponse::new;
    }

    @Override
    protected AsynchronousSearchResponse createTestInstance() {
        return new AsynchronousSearchResponse(UUID.randomUUID().toString(),
                getRandomAsynchronousSearchState(),
                randomNonNegativeLong(),
                randomNonNegativeLong(), getMockSearchResponse(), null);

    }

    @Override
    protected AsynchronousSearchResponse mutateInstance(AsynchronousSearchResponse instance) {
        return new AsynchronousSearchResponse(randomBoolean() ? instance.getId() : UUID.randomUUID().toString(),
                getRandomAsynchronousSearchState(),
                randomBoolean() ? instance.getStartTimeMillis() : randomNonNegativeLong(),
                randomBoolean() ? instance.getExpirationTimeMillis() : randomNonNegativeLong(),
                getMockSearchResponse(),
                instance.getError());
    }

    private AsynchronousSearchState getRandomAsynchronousSearchState() {
        return AsynchronousSearchState.values()[Randomness.get().nextInt(AsynchronousSearchState.values().length)];
    }

    private SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /*
     * we cannot compare the cause, because it will be wrapped and serialized in an outer
     * OpenSearchException best effort: try to check that the original
     * message appears somewhere in the rendered xContent.
     */
    public void testXContentRoundTripForAsynchronousSearchResponseContainingError() throws IOException {
        AsynchronousSearchResponse asResponse = new AsynchronousSearchResponse(UUID.randomUUID().toString(),
                getRandomAsynchronousSearchState(),
                randomNonNegativeLong(), randomNonNegativeLong(), null, new RuntimeException("test"));

        BytesReference serializedResponse;
        XContentType xContentType = Requests.INDEX_CONTENT_TYPE;
        serializedResponse = toXContent(asResponse, xContentType, true);
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, serializedResponse, xContentType)) {
            AsynchronousSearchResponse asResponse1 = AsynchronousSearchResponse.fromXContent(parser);
            String originalMsg = asResponse1.getError().getCause().getMessage();
            assertEquals(originalMsg,
                    "OpenSearch exception [type=runtime_exception, reason=test]");
        }
    }
}

