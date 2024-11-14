/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.listener;

import org.opensearch.search.asynchronous.utils.AsynchronousSearchAssertions;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class AsynchronousSearchProgressListenerIT extends OpenSearchSingleNodeTestCase {

    public void setUp() throws Exception {
        super.setUp();
        createRandomIndices(client());
    }

    public void testEmptyQueryString() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field1", "the quick brown fox jumps");
        SearchRequest searchRequest = new SearchRequest("test").source(new SearchSourceBuilder().query(queryStringQuery("quick")));
        testCase((NodeClient) client(), searchRequest);
    }

    public void testEmptyQueryStringNoDocs() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field1", "the quick brown fox jumps");
        SearchRequest searchRequest = new SearchRequest("test").source(new SearchSourceBuilder().query(queryStringQuery("")));
        testCase((NodeClient) client(), searchRequest);
    }

    public void testSearchProgressSimple() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType).source(new SearchSourceBuilder().size(0));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithHits() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType).source(new SearchSourceBuilder().size(10));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("max").field("number")));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithHitsAndAggs() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(10).aggregation(AggregationBuilders.max("max").field("number")));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithQuery() throws Exception {
        for (SearchType searchType : SearchType.values()) {
            SearchRequest request = new SearchRequest("index-*").searchType(searchType)
                .source(new SearchSourceBuilder().size(10).query(QueryBuilders.termQuery("foo", "bar")));
            testCase((NodeClient) client(), request);
        }
    }

    public void testSearchProgressWithShardSort() throws Exception {
        SearchRequest request = new SearchRequest("index-*").source(
            new SearchSourceBuilder().size(0).sort(new FieldSortBuilder("number").order(SortOrder.DESC))
        );
        request.setPreFilterShardSize(1);
        testCase((NodeClient) client(), request);
    }

    private void testCase(NodeClient client, SearchRequest request) throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsynchronousSearchProgressListenerIT.class.getName());
            SearchService service = getInstanceFromNode(SearchService.class);
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request.source());
            AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Function<SearchResponse, AsynchronousSearchResponse> responseFunction = (r) -> null;
            Function<Exception, AsynchronousSearchResponse> failureFunction = (e) -> null;
            AsynchronousSearchProgressListener listener = new AsynchronousSearchProgressListener(
                threadPool.relativeTimeInMillis(),
                responseFunction,
                failureFunction,
                threadPool.generic(),
                threadPool::relativeTimeInMillis,
                () -> reduceContextBuilder
            ) {
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
            AsynchronousSearchAssertions.assertSearchResponses(responseRef.get(), listener.partialResponse());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    private static List<SearchShard> createRandomIndices(Client client) {
        int numIndices = randomIntBetween(3, 20);
        for (int i = 0; i < numIndices; i++) {
            String indexName = String.format(Locale.ROOT, "index-%03d", i);
            assertAcked(client.admin().indices().prepareCreate(indexName).get());
            client.prepareIndex(indexName).setId(Integer.toString(i)).setSource("number", i, "foo", "bar").get();
        }
        client.admin().indices().prepareRefresh("index-*").get();
        ClusterSearchShardsResponse resp = client.admin().cluster().prepareSearchShards("index-*").get();
        return Arrays.stream(resp.getGroups()).map(e -> new SearchShard(null, e.getShardId())).sorted().collect(Collectors.toList());
    }
}
