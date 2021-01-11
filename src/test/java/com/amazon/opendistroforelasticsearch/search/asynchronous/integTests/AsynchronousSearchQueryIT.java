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

package com.amazon.opendistroforelasticsearch.search.asynchronous.integTests;

import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.TestClientUtils;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.sampler.Sampler;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sampler;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The intent of these tests is to verify that various elements of a SearchResponse, including aggregations, hits, highlighters, are
 * serialized and deserialized successfully
 */
@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(transportClientRatio = 0)// requires custom completion format
public class AsynchronousSearchQueryIT extends ESIntegTestCase {

    public static final int NUM_SHARDS = 2;
    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class, AsynchronousSearchPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class, AsynchronousSearchPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected int maximumNumberOfShards() {
        return 7;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    public void testEmptyQueryString() throws ExecutionException, InterruptedException, IOException {
        String index = UUID.randomUUID().toString();
        createIndex(index);
        indexRandom(true, client().prepareIndex(index, "type1", "1").setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(queryStringQuery("quick"));
        assertHitCount(getPersistedAsynchronousSearchResponse(searchRequest).getSearchResponse(), 3L);
        SearchRequest searchRequest1 = new SearchRequest(index);
        searchRequest1.source(new SearchSourceBuilder());
        searchRequest1.source().query(queryStringQuery(""));
        assertHitCount(getPersistedAsynchronousSearchResponse(searchRequest1).getSearchResponse(), 0L); // return no docs
    }

    public void testAggregationQuery() throws InterruptedException, ExecutionException {
        // Tests that we can refer to nested elements under a sample in a path
        // statement
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping(
                        "book", "author", "type=keyword", "name", "type=text", "genre",
                        "type=keyword", "price", "type=float"));
        createIndex("idx_unmapped");
        // idx_unmapped_author is same as main index but missing author field
        assertAcked(prepareCreate("idx_unmapped_author")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .addMapping("book", "name", "type=text", "genre", "type=keyword", "price", "type=float"));

        ensureGreen();
        String data[] = {
                // "id,cat,name,price,inStock,author_t,series_t,sequence_i,genre_s",
                "0553573403,book,A Game of Thrones,7.99,true,George R.R. Martin,A Song of Ice and Fire,1,fantasy",
                "0553579908,book,A Clash of Kings,7.99,true,George R.R. Martin,A Song of Ice and Fire,2,fantasy",
                "055357342X,book,A Storm of Swords,7.99,true,George R.R. Martin,A Song of Ice and Fire,3,fantasy",
                "0553293354,book,Foundation,17.99,true,Isaac Asimov,Foundation Novels,1,scifi",
                "0812521390,book,The Black Company,6.99,false,Glen Cook,The Chronicles of The Black Company,1,fantasy",
                "0812550706,book,Ender's Game,6.99,true,Orson Scott Card,Ender,1,scifi",
                "0441385532,book,Jhereg,7.95,false,Steven Brust,Vlad Taltos,1,fantasy",
                "0380014300,book,Nine Princes In Amber,6.99,true,Roger Zelazny,the Chronicles of Amber,1,fantasy",
                "0805080481,book,The Book of Three,5.99,true,Lloyd Alexander,The Chronicles of Prydain,1,fantasy",
                "080508049X,book,The Black Cauldron,5.99,true,Lloyd Alexander,The Chronicles of Prydain,2,fantasy"

        };

        for (int i = 0; i < data.length; i++) {
            String[] parts = data[i].split(",");
            client().prepareIndex("test", "book", "" + i)
                    .setSource("author", parts[5], "name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3])).get();
            client().prepareIndex("idx_unmapped_author", "book", "" + i)
                    .setSource("name", parts[2], "genre", parts[8], "price", Float.parseFloat(parts[3])).get();
        }
        client().admin().indices().refresh(new RefreshRequest("test")).get();
        boolean asc = randomBoolean();
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.types("book").searchType(SearchType.QUERY_THEN_FETCH);
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().aggregation(terms("genres")
                .field("genre")
                .order(BucketOrder.aggregation("sample>max_price.value", asc))
                .subAggregation(sampler("sample").shardSize(100)
                        .subAggregation(max("max_price").field("price"))));
        SearchResponse response = getPersistedAsynchronousSearchResponse(searchRequest).getSearchResponse();
        assertSearchResponse(response);
        Terms genres = response.getAggregations().get("genres");
        List<? extends Terms.Bucket> genreBuckets = genres.getBuckets();
        // For this test to be useful we need >1 genre bucket to compare
        assertThat(genreBuckets.size(), greaterThan(1));
        double lastMaxPrice = asc ? Double.MIN_VALUE : Double.MAX_VALUE;
        for (Terms.Bucket genreBucket : genres.getBuckets()) {
            Sampler sample = genreBucket.getAggregations().get("sample");
            Max maxPriceInGenre = sample.getAggregations().get("max_price");
            double price = maxPriceInGenre.getValue();
            if (asc) {
                assertThat(price, greaterThanOrEqualTo(lastMaxPrice));
            } else {
                assertThat(price, lessThanOrEqualTo(lastMaxPrice));
            }
            lastMaxPrice = price;
        }

    }

    public void testIpRangeQuery() throws InterruptedException {
        assertAcked(prepareCreate("idx")
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(true,
                client().prepareIndex("idx", "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex("idx", "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex("idx", "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        SearchRequest searchRequest = new SearchRequest("idx");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().aggregation(
                AggregationBuilders.ipRange("my_range")
                        .field("ip")
                        .addUnboundedTo("192.168.1.0")
                        .addRange("192.168.1.0", "192.168.1.10")
                        .addUnboundedFrom("192.168.1.10"));
        SearchResponse rsp = getPersistedAsynchronousSearchResponse(searchRequest).getSearchResponse();
        assertSearchResponse(rsp);
        Range range = rsp.getAggregations().get("my_range");
        assertEquals(3, range.getBuckets().size());

        Range.Bucket bucket1 = range.getBuckets().get(0);
        assertNull(bucket1.getFrom());
        assertEquals("192.168.1.0", bucket1.getTo());
        assertEquals("*-192.168.1.0", bucket1.getKey());
        assertEquals(0, bucket1.getDocCount());

        Range.Bucket bucket2 = range.getBuckets().get(1);
        assertEquals("192.168.1.0", bucket2.getFrom());
        assertEquals("192.168.1.10", bucket2.getTo());
        assertEquals("192.168.1.0-192.168.1.10", bucket2.getKey());
        assertEquals(1, bucket2.getDocCount());

        Range.Bucket bucket3 = range.getBuckets().get(2);
        assertEquals("192.168.1.10", bucket3.getFrom());
        assertNull(bucket3.getTo());
        assertEquals("192.168.1.10-*", bucket3.getKey());
        assertEquals(2, bucket3.getDocCount());
    }

    public void testHighlighterQuery() throws IOException, InterruptedException {
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
                .startObject("properties")
                .startObject("text")
                .field("type", "keyword")
                .field("store", true)
                .endObject()
                .endObject().endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test1")
                .addMapping("type", mappings));
        client().prepareIndex("test1", "type", "1")
                .setSource(jsonBuilder().startObject().field("text", "foo").endObject())
                .get();
        refresh();
        SearchRequest searchRequest = new SearchRequest("test1");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(matchQuery("text", "foo"))
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("text")));
        SearchResponse searchResponse = getPersistedAsynchronousSearchResponse(searchRequest).getSearchResponse();
        assertHighlight(searchResponse, 0, "text", 0, equalTo("<em>foo</em>"));
    }

    private AsynchronousSearchResponse getPersistedAsynchronousSearchResponse(SearchRequest searchRequest) throws InterruptedException {
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.keepOnCompletion(true);
        AsynchronousSearchResponse asResponse = TestClientUtils.blockingSubmitAsynchronousSearch(client(),
                request);
        TestClientUtils.assertResponsePersistence(client(), asResponse.getId());
        asResponse = TestClientUtils.blockingGetAsynchronousSearchResponse(client(),
                new GetAsynchronousSearchRequest(asResponse.getId()));
        return asResponse;
    }

    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {


        @Override
        public Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> getCharFilters() {
            return singletonMap("mock_pattern_replace", (indexSettings, env, name, settings) -> {
                class Factory implements NormalizingCharFilterFactory {

                    private final Pattern pattern = Regex.compile("[\\*\\?]", null);

                    @Override
                    public String name() {
                        return name;
                    }

                    @Override
                    public Reader create(Reader reader) {
                        return new PatternReplaceCharFilter(pattern, "", reader);
                    }
                }
                return new Factory();
            });
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return singletonMap("keyword", (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(name,
                    () -> new MockTokenizer(MockTokenizer.KEYWORD, false)));
        }
    }
}
