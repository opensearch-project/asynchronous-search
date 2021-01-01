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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.utils.AsyncSearchAssertions;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class AsyncSearchProgressListenerIT extends ESSingleNodeTestCase {

    public void setUp() throws Exception {
        super.setUp();
        createRandomIndices(client());
    }

    public void testEmptyQueryString() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox jumps");
        SearchRequest searchRequest = new SearchRequest("test").source(new SearchSourceBuilder().query(queryStringQuery("quick")));
        testCase((NodeClient) client(), searchRequest);
    }

    public void testEmptyQueryStringNoDocs() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", "the quick brown fox jumps");
        SearchRequest searchRequest = new SearchRequest("test").source(new SearchSourceBuilder().query(queryStringQuery("")));
        testCase((NodeClient) client(), searchRequest);
    }

    public void testSearchProgressSimple() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*")
                    .searchType(searchType)
                    .source(new SearchSourceBuilder().size(0));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithHits() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*")
                    .searchType(searchType)
                    .source(
                            new SearchSourceBuilder()
                                    .size(10)
                    );
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*")
                    .searchType(searchType)
                    .source(
                            new SearchSourceBuilder()
                                    .size(0)
                                    .aggregation(AggregationBuilders.max("max").field("number"))
                    );
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithHitsAndAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*")
                    .searchType(searchType)
                    .source(
                            new SearchSourceBuilder()
                                    .size(10)
                                    .aggregation(AggregationBuilders.max("max").field("number"))
                    );
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithQuery() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*")
                    .searchType(searchType)
                    .source(
                            new SearchSourceBuilder()
                                    .size(10)
                                    .query(QueryBuilders.termQuery("foo", "bar"))
                    );
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithShardSort() throws Exception {
        SearchRequest request = new SearchRequest("index-*")
                .source(
                        new SearchSourceBuilder()
                                .size(0)
                                .sort(new FieldSortBuilder("number").order(SortOrder.DESC))
                );
        request.setPreFilterShardSize(1);
        testCase((NodeClient) client(), request);
    }

    private void testCase(NodeClient client, SearchRequest request) throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsyncSearchProgressListenerIT.class.getName());
            SearchService service = getInstanceFromNode(SearchService.class);
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request);
            AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Function<SearchResponse, AsyncSearchResponse> responseFunction =
                    (r) -> null;
            Function<Exception, AsyncSearchResponse> failureFunction =
                    (e) -> null;
            AsyncSearchProgressListener listener = new AsyncSearchProgressListener(threadPool.relativeTimeInMillis(), responseFunction,
                    failureFunction, threadPool.generic(), threadPool::relativeTimeInMillis, () -> reduceContextBuilder){
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    assertTrue(responseRef.compareAndSet(null, searchResponse));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    assertTrue(exceptionRef.compareAndSet(null, exception));
                    latch.countDown();
                }
            };
            client.executeLocally(SearchAction.INSTANCE, new SearchRequest(request) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                    task.setProgressListener(listener);
                    return task;
                }
            }, listener);

            latch.await();
            AsyncSearchAssertions.assertSearchResponses(responseRef.get(), listener.partialResponse());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    private static List<SearchShard> createRandomIndices(Client client) {
        int numIndices = randomIntBetween(3, 20);
        for (int i = 0; i < numIndices; i++) {
            String indexName = String.format(Locale.ROOT, "index-%03d" , i);
            assertAcked(client.admin().indices().prepareCreate(indexName).get());
            client.prepareIndex(indexName, "doc", Integer.toString(i)).setSource("number", i, "foo", "bar").get();
        }
        client.admin().indices().prepareRefresh("index-*").get();
        ClusterSearchShardsResponse resp = client.admin().cluster().prepareSearchShards("index-*").get();
        return Arrays.stream(resp.getGroups())
                .map(e -> new SearchShard(null, e.getShardId()))
                .sorted()
                .collect(Collectors.toList());
    }
}
