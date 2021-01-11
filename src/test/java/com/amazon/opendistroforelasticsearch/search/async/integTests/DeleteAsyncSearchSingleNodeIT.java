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

package com.amazon.opendistroforelasticsearch.search.async.integTests;

import com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TriConsumer;
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

import static com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchSingleNodeTestCase.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class DeleteAsyncSearchSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    public void testDeleteAsyncSearchForRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsyncSearchNoRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                }, concurrentRuns);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsyncSearchPostCompletionNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(0, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns, numResourceNotFound.get());
                }, concurrentRuns);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsyncSearchPostCompletionForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get());
                }, concurrentRuns);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsyncSearchInBlockedStateForRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForBlockedSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns, plugins);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsyncSearchInBlockedStateForNoRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(false);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForBlockedSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(1, numDeleteAcknowledged.get());
                    assertEquals(concurrentRuns - 1, numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns, plugins);
        assertAsyncSearchResourceCleanUp(submitResponse.getId());
    }

    private void assertConcurrentDeletesForBlockedSearch(String id, TriConsumer<AtomicInteger, AtomicInteger,
            AtomicInteger> assertionConsumer, int concurrentRuns, List<SearchDelayPlugin> plugins) throws Exception {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search delete --->");
                    DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(id);
                    executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new LatchedActionListener<>
                            (new ActionListener<AcknowledgedResponse>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    if (acknowledgedResponse.isAcknowledged()) {
                                        numDeleteAcknowledged.incrementAndGet();
                                    } else {
                                        numDeleteUnAcknowledged.incrementAndGet();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof ResourceNotFoundException) {
                                        numResourceNotFound.incrementAndGet();
                                    }
                                }
                            }, countDownLatch));
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            disableBlocks(plugins);
            assertionConsumer.apply(numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void assertConcurrentDeletes(String id, TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                         int concurrentRuns) throws InterruptedException {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsyncSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering async search delete --->");
                    DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(id);
                    executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest, new LatchedActionListener<>
                            (new ActionListener<AcknowledgedResponse>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    if (acknowledgedResponse.isAcknowledged()) {
                                        numDeleteAcknowledged.incrementAndGet();
                                    } else {
                                        numDeleteUnAcknowledged.incrementAndGet();
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof ResourceNotFoundException) {
                                        numResourceNotFound.incrementAndGet();
                                    }
                                }
                            }, countDownLatch));
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            assertionConsumer.apply(numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}
