/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.integTests;

import org.opensearch.search.asynchronous.commons.AsynchronousSearchSingleNodeTestCase;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.utils.QuadConsumer;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MixedOperationSingleNodeIT extends AsynchronousSearchSingleNodeTestCase {

    public void testGetAndDeleteAsynchronousSearchForRetainedResponse() throws InterruptedException {
        try {
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
            assertConcurrentGetOrUpdatesWithDeletes(
                submitResponse,
                (numSuccess, numGetFailures, numVersionConflictFailure, numResourceNotFoundFailures) -> {
                    assertEquals(concurrentRuns, numSuccess.get() + numResourceNotFoundFailures.get() + numVersionConflictFailure.get());
                    assertEquals(0, numGetFailures.get());
                },
                false,
                concurrentRuns,
                true
            );
            assertAsynchronousSearchResourceCleanUp(submitResponse.getId());
        } finally {
            CountDownLatch deleteLatch = new CountDownLatch(1);
            client().admin()
                .indices()
                .prepareDelete(INDEX)
                .execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> { deleteLatch.countDown(); }));
            deleteLatch.await();
        }
    }

    private void assertConcurrentGetOrUpdatesWithDeletes(
        AsynchronousSearchResponse submitResponse,
        QuadConsumer<AtomicInteger, AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer,
        boolean update,
        int concurrentRuns,
        boolean retainResponse
    ) throws InterruptedException {
        AtomicInteger numSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        AtomicInteger numResourceNotFoundFailures = new AtomicInteger();
        AtomicInteger numTimeouts = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsynchronousSearchSingleNodeIT.class.getName());
            int numThreads = concurrentRuns;
            long lowerKeepAliveMillis = 5 * 1000 * 60 * 60; // 5 hours in millis
            long higherKeepAliveMillis = 10 * 1000 * 60 * 60; // 10 hours in millis
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            long randomDeleteThread = randomLongBetween(0, numThreads - 1);
            for (int i = 0; i < numThreads; i++) {
                long keepAlive = randomLongBetween(lowerKeepAliveMillis, higherKeepAliveMillis);
                int currentThreadIteration = i;
                Runnable thread = () -> {
                    if (currentThreadIteration == randomDeleteThread) {
                        DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(
                            submitResponse.getId()
                        );
                        executeDeleteAsynchronousSearch(
                            client(),
                            deleteAsynchronousSearchRequest,
                            new LatchedActionListener<>(new ActionListener<AcknowledgedResponse>() {
                                @Override
                                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                    assertTrue(acknowledgedResponse.isAcknowledged());
                                    numSuccess.incrementAndGet();
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof OpenSearchTimeoutException) {
                                        numTimeouts.incrementAndGet();
                                    } else {
                                        fail("Unexpected exception " + e.getMessage());
                                    }
                                }
                            }, countDownLatch)
                        );
                    } else {
                        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(
                            submitResponse.getId()
                        );
                        long requestedTime = System.currentTimeMillis() + keepAlive;
                        if (update) {
                            logger.info("Triggering asynchronous search gets with keep alive [{}] --->", requestedTime);
                            getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueMillis(keepAlive));
                        }
                        getAsynchronousSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
                        executeGetAsynchronousSearch(
                            client(),
                            getAsynchronousSearchRequest,
                            new LatchedActionListener<>(new ActionListener<AsynchronousSearchResponse>() {
                                @Override
                                public void onResponse(AsynchronousSearchResponse asResponse) {
                                    if (update) {
                                        assertThat(
                                            asResponse.getExpirationTimeMillis(),
                                            greaterThanOrEqualTo(System.currentTimeMillis() + lowerKeepAliveMillis)
                                        );
                                        assertThat(
                                            asResponse.getExpirationTimeMillis(),
                                            lessThanOrEqualTo(System.currentTimeMillis() + higherKeepAliveMillis)
                                        );
                                    }
                                    numSuccess.incrementAndGet();
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
                            }, countDownLatch)
                        );
                    }
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
            if (retainResponse && update) {
                assertEquals(numTimeouts.get(), 0);
                assertionConsumer.apply(numSuccess, numGetFailures, numVersionConflictFailures, numTimeouts);
            } else {
                assertionConsumer.apply(numSuccess, numGetFailures, numVersionConflictFailures, numResourceNotFoundFailures);
            }
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }
}
