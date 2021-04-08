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
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.QuadConsumer;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.TriConsumer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchSingleNodeTestCase.SearchDelayPlugin.SCRIPT_NAME;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GetAsynchronousSearchSingleNodeIT extends AsynchronousSearchSingleNodeTestCase {

    public void testGetAsynchronousSearchForNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailure, numResourceNotFoundFailures) -> {
                    assertEquals(concurrentRuns, numGetSuccess.get() + numResourceNotFoundFailures.get()
                            + numVersionConflictFailure.get());
                    assertEquals(0, numGetFailures.get());
                }, false, concurrentRuns, false);
    }

    public void testUpdateAsynchronousSearchForNoRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get()
                            + numResourceNotFoundFailures.get());
                }, true, concurrentRuns, false);
    }

    public void testUpdateAsynchronousSearchForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numTimeouts) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get()
                            + numTimeouts.get());
                }, true, concurrentRuns, true);
    }

    public void testGetAsynchronousSearchForRetainedResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(20, 50);
        assertConcurrentGetOrUpdates(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numResourceNotFoundFailures.get()
                            + numVersionConflictFailures.get());
                }, false, concurrentRuns, true);
    }

    public void testGetAsynchronousSearchInBlockedStateForNoRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numResourceNotFound) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get());
                    assertEquals(0, numResourceNotFound.get());
                }, false, concurrentRuns, false, plugins);
    }

    public void testGetAsynchronousSearchInBlockedStateForRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numResourceNotFound) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get());
                    assertEquals(0, numResourceNotFound.get());
                }, false, concurrentRuns, false, plugins);
    }

    public void testUpdateAsynchronousSearchInBlockedStateForRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get());
                }, true, concurrentRuns, false, plugins);
    }

    public void testUpdateAsynchronousSearchInBlockedStateForNoRetainedResponse() throws Exception {
        List<SearchDelayPlugin> plugins = initPluginFactory();
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder());
        searchRequest.source().query(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest)
        .actionGet();
        assertNotNull(submitResponse);
        int concurrentRuns = randomIntBetween(10, 20);
        assertConcurrentGetForBlockedSearch(submitResponse,
                (numGetSuccess, numGetFailures, numVersionConflictFailures) -> {
                    assertEquals(0, numGetFailures.get());
                    assertEquals(concurrentRuns, numGetSuccess.get() + numVersionConflictFailures.get());
                }, true, concurrentRuns, false, plugins);
    }

    private void assertConcurrentGetForBlockedSearch(AsynchronousSearchResponse submitResponse,
                                                     TriConsumer<AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                                     boolean update, int concurrentRuns, boolean retainResponse,
                                                     List<SearchDelayPlugin> plugins) throws Exception {

        AtomicInteger numGetSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsynchronousSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            long lowerKeepAliveMillis = 5 * 1000 * 60 * 60 ;
            long higherKeepAliveMillis = 10 * 1000 * 60 * 60;
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable thread = () -> {
                    GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
                    if (update) {
                        logger.info("Triggering asynchronous search gets with keep alives --->");
                        getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueMillis(randomLongBetween(lowerKeepAliveMillis,
                                higherKeepAliveMillis)));
                    }
                    //if waitForCompletionTimeout is null we return response immediately
                    TimeValue waitForCompletionTimeout = randomBoolean() ? null :
                            TimeValue.timeValueMillis(randomLongBetween(1, 5000));
                    getAsynchronousSearchRequest.setWaitForCompletionTimeout(waitForCompletionTimeout);
                    executeGetAsynchronousSearch(client(), getAsynchronousSearchRequest,
                    new ActionListener<AsynchronousSearchResponse>() {
                        @Override
                        public void onResponse(AsynchronousSearchResponse acknowledgedResponse) {
                            numGetSuccess.incrementAndGet();
                            countDownLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                numVersionConflictFailures.incrementAndGet();
                            } else {
                                numGetFailures.incrementAndGet();
                            }
                            countDownLatch.countDown();
                        }
                    });
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(submitResponse.getId());
            CountDownLatch deleteLatch = new CountDownLatch(1);
            executeDeleteAsynchronousSearch(client(), deleteAsynchronousSearchRequest,
            ActionListener.wrap(() -> deleteLatch.countDown()));
            deleteLatch.await();
            disableBlocks(plugins);
            assertionConsumer.apply(numGetSuccess, numGetFailures, numVersionConflictFailures);
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void assertConcurrentGetOrUpdates(AsynchronousSearchResponse submitResponse,
                                              QuadConsumer<AtomicInteger, AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
                                              boolean update, int concurrentRuns, boolean retainResponse)
            throws InterruptedException {
        AtomicInteger numGetSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        AtomicInteger numResourceNotFoundFailures = new AtomicInteger();
        AtomicInteger numTimeouts = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsynchronousSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            long lowerKeepAliveMillis = 5 * 1000 * 60 * 60 ; // 5 hours in millis
            long higherKeepAliveMillis = 10 * 1000 * 60 * 60; // 10 hours in millis
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                long keepAlive = randomLongBetween(lowerKeepAliveMillis, higherKeepAliveMillis);
                Runnable thread = () -> {
                    GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
                    long requestedTime = System.currentTimeMillis() + keepAlive;
                    if (update) {
                        logger.info("Triggering asynchronous search gets with keep alive [{}] --->", requestedTime);
                        getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueMillis(keepAlive));
                    }
                    getAsynchronousSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
                    executeGetAsynchronousSearch(client(), getAsynchronousSearchRequest, new LatchedActionListener<>(
                            new ActionListener<AsynchronousSearchResponse>() {
                        @Override
                        public void onResponse(AsynchronousSearchResponse asResponse) {
                            if (update) {
                                // while updates we can run into version conflicts and hence the comparison is on the successful
                                // response. Since the final keep alive is calculated based on the current time of the server
                                // active contexts's expiration in memory are superseded by later writer so we are keeping a loose
                                // check
                                assertThat(asResponse.getExpirationTimeMillis(), greaterThanOrEqualTo(
                                        System.currentTimeMillis() + lowerKeepAliveMillis));
                                assertThat(asResponse.getExpirationTimeMillis(), lessThanOrEqualTo(
                                        System.currentTimeMillis() + higherKeepAliveMillis));
                            }
                            numGetSuccess.incrementAndGet();
                        }
                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof VersionConflictEngineException) {
                                numVersionConflictFailures.incrementAndGet();
                            } else if (e instanceof ResourceNotFoundException) {
                                numResourceNotFoundFailures.incrementAndGet();
                            } else if (e instanceof OpenSearchTimeoutException) {
                                numTimeouts.incrementAndGet();
                            } else {
                                numGetFailures.incrementAndGet();
                            }
                        }
                    }, countDownLatch));
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            if (retainResponse) {
                DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(
                        submitResponse.getId());
                executeDeleteAsynchronousSearch(client(), deleteAsynchronousSearchRequest).actionGet();
            }
            if (retainResponse && update) {
                assertionConsumer.apply(numGetSuccess, numGetFailures, numVersionConflictFailures, numTimeouts);
            } else {
                assertionConsumer.apply(numGetSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures);
            }

        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}

