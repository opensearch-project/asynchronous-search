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

import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
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
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchSingleNodeTestCase.SearchDelayPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;

public class DeleteAsynchronousSearchSingleNodeIT extends AsynchronousSearchSingleNodeTestCase {

    public void testDeleteAsynchronousSearchForRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertThat(numDeleteAcknowledged.get(), Matchers.greaterThan(0));
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsynchronousSearchNoRetainedResponseRandomTime() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                }, concurrentRuns);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsynchronousSearchPostCompletionNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertEquals(0, numDeleteAcknowledged.get());
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns, numResourceNotFound.get());
                }, concurrentRuns);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsynchronousSearchPostCompletionForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentDeletes(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertThat(numDeleteAcknowledged.get(), Matchers.greaterThan(0));
                    assertEquals(0, numDeleteUnAcknowledged.get());
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsynchronousSearchInBlockedStateForRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForBlockedSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertThat(numDeleteAcknowledged.get(), Matchers.greaterThan(0));
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns, plugins);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    public void testDeleteAsynchronousSearchInBlockedStateForNoRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest).actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentDeletesForBlockedSearch(submitResponse.getId(),
                (numDeleteAcknowledged, numDeleteUnAcknowledged, numResourceNotFound) -> {
                    assertThat(numDeleteAcknowledged.get(), Matchers.greaterThan(0));
                    assertEquals(concurrentRuns, numDeleteAcknowledged.get() + numResourceNotFound.get() + numDeleteUnAcknowledged.get());
                }, concurrentRuns, plugins);
        assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
    }

    private void assertConcurrentDeletesForBlockedSearch(String id, TriConsumer<AtomicInteger, AtomicInteger,
            AtomicInteger> assertionConsumer, int concurrentRuns, List<SearchDelayPlugin> plugins) throws Exception {
        AtomicInteger numDeleteAcknowledged = new AtomicInteger();
        AtomicInteger numDeleteUnAcknowledged = new AtomicInteger();
        AtomicInteger numResourceNotFound = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(DeleteAsynchronousSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering asynchronous search delete --->");
                    DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(id);
                    executeDeleteAsynchronousSearch(client(), deleteAsynchronousSearchRequest, new LatchedActionListener<>
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
            testThreadPool = new TestThreadPool(DeleteAsynchronousSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering asynchronous search delete --->");
                    DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(id);
                    executeDeleteAsynchronousSearch(client(), deleteAsynchronousSearchRequest, new LatchedActionListener<>
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
