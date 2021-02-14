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

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListenerIT;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.AsynchronousSearchAssertions;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, transportClientRatio = 0)
public class AsynchronousSearchRejectionIT extends AsynchronousSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("thread_pool.search.size", 1)
                .put("thread_pool.search.queue_size", 1)
                .put("thread_pool.write.size", 1)
                .put("thread_pool.write.queue_size", 10)
                .put("thread_pool.get.size", 1)
                .put("thread_pool.get.queue_size", 10)
                .put(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(), true)
                .build();
    }

    @TestLogging(value = "_root:DEBUG", reason = "flaky")
    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "1").get();
        }
        AtomicInteger numRejections = new AtomicInteger();
        AtomicInteger numRnf = new AtomicInteger();
        AtomicInteger numTimeouts = new AtomicInteger();
        AtomicInteger numFailures = new AtomicInteger();
        int numberOfAsyncOps = randomIntBetween(100, 200);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps * 2);
        final CopyOnWriteArrayList<Object> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numberOfAsyncOps; i++) {
            SearchRequest request = client().prepareSearch("test")
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchQuery("field", "1"))
                    .request();
            SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(request);
            submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMinutes(1));
            boolean keepOnCompletion = randomBoolean();
            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
                    client().execute(SubmitAsynchronousSearchAction.INSTANCE, submitAsynchronousSearchRequest,
                            new LatchedActionListener<>(new ActionListener<AsynchronousSearchResponse>() {
                                @Override
                                public void onResponse(AsynchronousSearchResponse asynchronousSearchResponse) {
                                    if (asynchronousSearchResponse.getSearchResponse() != null) {
                                        responses.add(asynchronousSearchResponse.getSearchResponse());
                                    } else if(asynchronousSearchResponse.getError() != null){
                                        responses.add(asynchronousSearchResponse.getError());
                                    }
                                    if (asynchronousSearchResponse.getId() == null) {
                                        // task cancelled by the time we process final response/error due to during partial merge failure.
                                        // no  delete required
                                        latch.countDown();
                                    } else {
                                        DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest
                                                = new DeleteAsynchronousSearchRequest(asynchronousSearchResponse.getId());
                                        client().execute(DeleteAsynchronousSearchAction.INSTANCE, deleteAsynchronousSearchRequest,
                                                new LatchedActionListener<>(new ActionListener<AcknowledgedResponse>() {
                                                    @Override
                                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                                        assertTrue(acknowledgedResponse.isAcknowledged());
                                                    }

                                                    @Override
                                                    public void onFailure(Exception e) {
                                                        Throwable cause = ExceptionsHelper.unwrapCause(e);
                                                        if (cause instanceof EsRejectedExecutionException) {
                                                            numRejections.incrementAndGet();
                                                        } else if (cause instanceof ElasticsearchTimeoutException) {
                                                            numTimeouts.incrementAndGet();
                                                        } else if(cause instanceof ResourceNotFoundException) {
                                                            // deletion is in race with task cancellation due to partial merge failure
                                                            numRnf.getAndIncrement();
                                                        } else {
                                                            numFailures.incrementAndGet();
                                                        }
                                                    }
                                                }, latch));
                                    }
                        }
                        @Override
                        public void onFailure(Exception e) {
                            responses.add(e);
                            assertThat(e.getMessage(), startsWith("Trying to create too many concurrent searches"));
                            latch.countDown();
                        }
                    }, latch));
        }
        latch.await();


        // validate all responses
        for (Object response : responses) {
            if (response instanceof SearchResponse) {
                SearchResponse searchResponse = (SearchResponse) response;
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    assertTrue("got unexpected reason..." + failure.reason(),
                            failure.reason().toLowerCase(Locale.ENGLISH).contains("rejected"));
                }
            } else {
                Exception t = (Exception) response;
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException) {
                    SearchPhaseExecutionException e = (SearchPhaseExecutionException) unwrap;
                    for (ShardSearchFailure failure : e.shardFailures()) {
                        assertTrue("got unexpected reason..." + failure.reason(),
                        // task cancellation can occur due to partial merge failures
                                failure.reason().toLowerCase(Locale.ENGLISH).contains("cancelled") ||
                                failure.reason().toLowerCase(Locale.ENGLISH).contains("rejected"));
                    }
                    // we have have null responses if submit completes before search starts
                } else if (unwrap instanceof EsRejectedExecutionException == false) {
                    throw new AssertionError("unexpected failure + ", (Throwable) response);
                }
            }
        }
        assertThat(responses.size(), equalTo(numberOfAsyncOps));
        assertThat(numFailures.get(), equalTo(0));
    }

    public void testSearchFailures() throws Exception {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "1").get();
        }
        int numberOfAsyncOps = randomIntBetween(100, 200);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final CopyOnWriteArrayList<Object> responses = new CopyOnWriteArrayList<>();
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsynchronousSearchProgressListenerIT.class.getName());
            for (int i = 0; i < numberOfAsyncOps; i++) {
                SearchRequest request = client().prepareSearch("test")
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        .setQuery(QueryBuilders.matchQuery("field", "1"))
                        .request();
                AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
                AtomicInteger reduceContextInvocation = new AtomicInteger();
                AsynchronousSearchProgressListener listener;
                SearchService service = internalCluster().getInstance(SearchService.class);
                InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request);
                AtomicReference<Exception> exceptionRef = new AtomicReference<>();
                Function<SearchResponse, AsynchronousSearchResponse> responseFunction =
                        (r) -> null;
                Function<Exception, AsynchronousSearchResponse> failureFunction =
                        (e) -> null;
                listener = new AsynchronousSearchProgressListener(threadPool.relativeTimeInMillis(), responseFunction,
                        failureFunction, threadPool.generic(), threadPool::relativeTimeInMillis,
                        () -> reduceContextBuilder) {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        assertTrue(responseRef.compareAndSet(null, searchResponse));
                        AsynchronousSearchAssertions.assertSearchResponses(searchResponse, this.partialResponse());
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        assertTrue(exceptionRef.compareAndSet(null, exception));
                        latch.countDown();
                    }
                };
                client().execute(SearchAction.INSTANCE, new SearchRequest(request) {
                    @Override
                    public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                        SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                        task.setProgressListener(listener);
                        return task;
                    }
                }, listener);
            }
            latch.await();
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
}
