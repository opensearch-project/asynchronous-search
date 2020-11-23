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

package com.amazon.opendistroforelasticsearch.search.async.response;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class AsyncSearchResponseTests extends AbstractSerializingTestCase<AsyncSearchResponse> {

    @Override
    protected AsyncSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return AsyncSearchResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AsyncSearchResponse> instanceReader() {
        return AsyncSearchResponse::new;
    }

    @Override
    protected AsyncSearchResponse createTestInstance() {
        return new AsyncSearchResponse(UUID.randomUUID().toString(), randomBoolean(), randomNonNegativeLong(),
                randomNonNegativeLong(), getMockSearchResponse(), null);

    }

    @Override
    protected AsyncSearchResponse mutateInstance(AsyncSearchResponse instance) {
        return new AsyncSearchResponse(randomBoolean() ? instance.getId() : UUID.randomUUID().toString(),
                randomBoolean() == instance.isRunning(),
                randomBoolean() ? instance.getStartTimeMillis() : randomNonNegativeLong(),
                randomBoolean() ? instance.getExpirationTimeMillis() : randomNonNegativeLong(),
                getMockSearchResponse(),
                instance.getError());
    }

    private SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, randomInt(5)),
                "", totalShards, successfulShards, 0, randomNonNegativeLong(),
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /*
     * we cannot compare the cause, because it will be wrapped and serialized in an outer
     * ElasticSearchException best effort: try to check that the original
     * message appears somewhere in the rendered xContent.
     */
    public void testXContentRoundTripForAsyncSearchResponseContainingError() throws IOException {
        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(UUID.randomUUID().toString(), randomBoolean(),
                randomNonNegativeLong(), randomNonNegativeLong(), null, new RuntimeException("test"));

        BytesReference serializedResponse;
        XContentType xContentType = Requests.INDEX_CONTENT_TYPE;
        serializedResponse = toXContent(asyncSearchResponse, xContentType, true);
        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, serializedResponse, xContentType)) {
            AsyncSearchResponse asyncSearchResponse1 = AsyncSearchResponse.fromXContent(parser);
            String originalMsg = asyncSearchResponse1.getError().getCause().getMessage();
            assertEquals(originalMsg,
                    "Elasticsearch exception [type=runtime_exception, reason=test]");
        }
    }
}

