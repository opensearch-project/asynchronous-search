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

package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, transportClientRatio = 0)
public class AsyncSearchRejectionIT extends AsyncSearchIntegTestCase {

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
                .build();
    }


    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "1").get();
        }
        AtomicInteger numRejections = new AtomicInteger();
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
            SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(request);
            submitAsyncSearchRequest.keepOnCompletion(true);
                    client().execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest,
                            new LatchedActionListener<>(new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (asyncSearchResponse.getSearchResponse() == null) {
                                responses.add(asyncSearchResponse.getError());
                            } else {
                                responses.add(asyncSearchResponse.getSearchResponse());
                            }
                            DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(
                                    asyncSearchResponse.getId());
                            client().execute(DeleteAsyncSearchAction.INSTANCE, deleteAsyncSearchRequest,
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
                                    } else {
                                        numFailures.incrementAndGet();
                                    }
                                }
                            }, latch));
                        }
                        @Override
                        public void onFailure(Exception e) {
                            responses.add(e);
                            assertThat(e.getMessage(), startsWith("Trying to create too many running contexts"));
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
                                failure.reason().toLowerCase(Locale.ENGLISH).contains("rejected"));
                    }
                } else if ((unwrap instanceof EsRejectedExecutionException) == false) {
                    throw new AssertionError("unexpected failure", (Throwable) response);
                }
            }
        }
        assertThat(responses.size(), equalTo(numberOfAsyncOps));
        assertThat(numFailures.get(), equalTo(0));
    }
}