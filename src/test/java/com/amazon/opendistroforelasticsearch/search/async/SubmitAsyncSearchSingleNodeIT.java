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

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchRejectedException;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchSingleNodeTestCase.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class SubmitAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    private int asyncSearchConcurrentLimit = 60;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("async_search.max_running_context", asyncSearchConcurrentLimit).build();
    }

    public void testSubmitAsyncSearchWithoutRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentSubmits(submitAsyncSearchRequest, searchResponse, (numStartedAsyncSearch, numFailedAsyncSearch,
                                                                           numErrorResponseAsyncSearch) -> {
            assertEquals(concurrentRuns, numStartedAsyncSearch.get());
            assertEquals(0,  numFailedAsyncSearch.get());
            assertEquals(0,  numErrorResponseAsyncSearch.get());
        }, concurrentRuns);
    }

    public void testSubmitAsyncSearchWithRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentSubmits(submitAsyncSearchRequest, searchResponse, (numStartedAsyncSearch, numFailedAsyncSearch,
                                                                           numErrorResponseAsyncSearch) -> {
            assertEquals(concurrentRuns, numStartedAsyncSearch.get());
            assertEquals(0,  numFailedAsyncSearch.get());
            assertEquals(0,  numErrorResponseAsyncSearch.get());
        }, concurrentRuns);
    }

    public void testSubmitAsyncSearchWithNoRetainedResponseBlocking() throws Exception {
        int concurrentRuns = randomIntBetween(asyncSearchConcurrentLimit + 10, asyncSearchConcurrentLimit + 20);
        assertConcurrentSubmitsForBlockedSearch((numStartedAsyncSearch, numFailedAsyncSearch, numRejectedAsyncSearch) -> {
            assertEquals(asyncSearchConcurrentLimit, numStartedAsyncSearch.get());
            assertEquals(concurrentRuns - asyncSearchConcurrentLimit,  numFailedAsyncSearch.get());
            assertEquals(concurrentRuns - asyncSearchConcurrentLimit,  numRejectedAsyncSearch.get());
        }, concurrentRuns);
    }

    private void assertConcurrentSubmitsForBlockedSearch(TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                                         int concurrentRuns) throws Exception {
        AtomicInteger numStartedAsyncSearch = new AtomicInteger();
        AtomicInteger numFailedAsyncSearch = new AtomicInteger();
        AtomicInteger numRejectedAsyncSearch = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        List<SearchDelayPlugin> plugins = initPluginFactory();
        try {
            testThreadPool = new TestThreadPool(SubmitAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search submit --->");
                    SearchRequest searchRequest = new SearchRequest("index");
                    searchRequest.source(new SearchSourceBuilder());
                    searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME,
                            Collections.emptyMap())));
                    SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
                    submitAsyncSearchRequest.keepOnCompletion(false);
                    submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
                    executeSubmitAsyncSearch(client(), submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (asyncSearchResponse.getId() != null) {
                                numStartedAsyncSearch.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof AsyncSearchRejectedException) {
                                numRejectedAsyncSearch.incrementAndGet();
                            }
                            numFailedAsyncSearch.incrementAndGet();
                            countDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            disableBlocks(plugins);
            assertionConsumer.apply(numStartedAsyncSearch, numFailedAsyncSearch, numRejectedAsyncSearch);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void assertConcurrentSubmits(SubmitAsyncSearchRequest submitAsyncSearchRequest, SearchResponse searchResponse,
                                         TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer, int concurrentRuns)
            throws InterruptedException {
        AtomicInteger numStartedAsyncSearch = new AtomicInteger();
        AtomicInteger numFailedAsyncSearch = new AtomicInteger();
        AtomicInteger numErrorResponseAsyncSearch = new AtomicInteger();
        final ClusterState state = getInstanceFromNode(ClusterService.class).state();

        TestThreadPool testThreadPool = null;
        CountDownLatch countDownLatch;
        try {
            testThreadPool = new TestThreadPool(SubmitAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            if (submitAsyncSearchRequest.getKeepOnCompletion()) {
                //we also need to delete async search response to ensure test completes gracefully with no background tasks
                // running
                countDownLatch = new CountDownLatch(2 * numThreads);
            } else {
                countDownLatch = new CountDownLatch(numThreads);
            }

            for (int i = 0; i < numThreads; i++) {
                CountDownLatch finalCountDownLatch = countDownLatch;
                Runnable thread = () -> {
                    logger.info("Triggering async search submit --->");
                    executeSubmitAsyncSearch(client(), submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
                        @Override
                        public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                            if (asyncSearchResponse.getId() != null) {
                                AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(asyncSearchResponse.getId());
                                assertEquals(state.nodes().getLocalNodeId(), asyncSearchId.getNode());
                                AsyncSearchAssertions.assertSearchResponses(searchResponse, asyncSearchResponse.getSearchResponse());
                                numStartedAsyncSearch.incrementAndGet();
                            }
                            if (asyncSearchResponse.getError() != null) {
                                numErrorResponseAsyncSearch.incrementAndGet();
                            }
                            finalCountDownLatch.countDown();

                            if (submitAsyncSearchRequest.getKeepOnCompletion()) {
                                DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(
                                        asyncSearchResponse.getId());
                                executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
                                    @Override
                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                        assertTrue(acknowledgedResponse.isAcknowledged());
                                        finalCountDownLatch.countDown();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        fail("Search deletion failed for async search id "+ asyncSearchResponse.getId());
                                        finalCountDownLatch.countDown();
                                    }
                                });
                            };
                        }
                        @Override
                        public void onFailure(Exception e) {
                            numFailedAsyncSearch.incrementAndGet();
                            finalCountDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertionConsumer.apply(numStartedAsyncSearch, numFailedAsyncSearch, numErrorResponseAsyncSearch);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}
