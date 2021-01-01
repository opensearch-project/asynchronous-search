package com.amazon.opendistroforelasticsearch.search.async.integTests;

import com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.QuadConsumer;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MixedOperationSingleNodeIT extends AsyncSearchSingleNodeTestCase {

    public void testGetAndDeleteAsyncSearchForRetainedResponse() throws InterruptedException {
        try {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("index");
            searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value0")));
            SubmitAsyncSearchRequest submitAsyncSearchRequest = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
            submitAsyncSearchRequest.keepOnCompletion(true);
            submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
            AsyncSearchResponse submitResponse = executeSubmitAsyncSearch(client(), submitAsyncSearchRequest).actionGet();
            assertNotNull(submitResponse);
            int concurrentRuns = randomIntBetween(20, 50);
            assertConcurrentGetOrUpdatesWithDeletes(submitResponse,
                    (numSuccess, numGetFailures, numVersionConflictFailure, numResourceNotFoundFailures) -> {
                        assertEquals(concurrentRuns, numSuccess.get() + numResourceNotFoundFailures.get()
                                + numVersionConflictFailure.get());
                        assertEquals(0, numGetFailures.get());
                    }, false, concurrentRuns, true);
            assertAsyncSearchResourceCleanUp(submitResponse.getId());
        } finally {
            CountDownLatch deleteLatch = new CountDownLatch(1);
            client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
                deleteLatch.countDown();
            }));
            deleteLatch.await();
        }
    }

    private void assertConcurrentGetOrUpdatesWithDeletes(AsyncSearchResponse submitResponse, QuadConsumer<AtomicInteger,
            AtomicInteger, AtomicInteger, AtomicInteger> assertionConsumer, boolean update, int concurrentRuns, boolean retainResponse)
            throws InterruptedException {
        AtomicInteger numSuccess = new AtomicInteger();
        AtomicInteger numGetFailures = new AtomicInteger();
        AtomicInteger numVersionConflictFailures = new AtomicInteger();
        AtomicInteger numResourceNotFoundFailures = new AtomicInteger();
        AtomicInteger numTimeouts = new AtomicInteger();
        TestThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(GetAsyncSearchSingleNodeIT.class.getName());
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
                        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(submitResponse.getId());
                        executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest,
                                new LatchedActionListener<>(new ActionListener<AcknowledgedResponse>() {
                                    @Override
                                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                                        assertTrue(acknowledgedResponse.isAcknowledged());
                                        numSuccess.incrementAndGet();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        if (e instanceof ElasticsearchTimeoutException) {
                                            numTimeouts.incrementAndGet();
                                        } else {
                                            fail("Unexpected exception " + e.getMessage());
                                        }
                                    }
                                }, countDownLatch));
                    } else {
                        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(submitResponse.getId());
                        long requestedTime = System.currentTimeMillis() + keepAlive;
                        if (update) {
                            logger.info("Triggering async search gets with keep alive [{}] --->", requestedTime);
                            getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(keepAlive));
                        }
                        getAsyncSearchRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 5000)));
                        executeGetAsyncSearch(client(), getAsyncSearchRequest, new LatchedActionListener<>(
                                new ActionListener<AsyncSearchResponse>() {
                                    @Override
                                    public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                                        if (update) {
                                            assertThat(asyncSearchResponse.getExpirationTimeMillis(), greaterThanOrEqualTo(
                                                    System.currentTimeMillis() + lowerKeepAliveMillis));
                                            assertThat(asyncSearchResponse.getExpirationTimeMillis(), lessThanOrEqualTo(
                                                    System.currentTimeMillis() + higherKeepAliveMillis));
                                        }
                                        numSuccess.incrementAndGet();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        if (e instanceof VersionConflictEngineException) {
                                            numVersionConflictFailures.incrementAndGet();
                                        } else if (e instanceof ResourceNotFoundException) {
                                            numResourceNotFoundFailures.incrementAndGet();
                                        } else if (e instanceof ElasticsearchTimeoutException) {
                                            numTimeouts.incrementAndGet();
                                        } else {
                                            numGetFailures.incrementAndGet();
                                        }
                                    }
                                }, countDownLatch));
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
