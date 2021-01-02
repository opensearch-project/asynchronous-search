package com.amazon.opendistroforelasticsearch.search.async.restIT;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore.DEFAULT_MAX_RUNNING_CONTEXTS;
import static org.hamcrest.Matchers.containsString;

public class AsyncSearchSettingsIT extends AsyncSearchRestTestCase {

    public void testMaxKeepAliveSetting() throws Exception {
        try {
            SubmitAsyncSearchRequest validRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            validRequest.keepAlive(TimeValue.timeValueDays(2));
            AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(validRequest);
            assertNotNull(asyncSearchResponse.getSearchResponse());
            updateClusterSettings(AsyncSearchService.MAX_KEEP_ALIVE_SETTING.getKey(), TimeValue.timeValueDays(1));
            SubmitAsyncSearchRequest invalidRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            invalidRequest.keepAlive(TimeValue.timeValueDays(2));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(invalidRequest));
            assertThat(responseException.getMessage(), containsString("Keep alive for async search (" +
                    invalidRequest.getKeepAlive().getMillis() + ") is too large"));
            updateClusterSettings(AsyncSearchService.MAX_KEEP_ALIVE_SETTING.getKey(), TimeValue.timeValueDays(10));
        } finally {
            deleteIndexIfExists();
        }
    }

    public void testSubmitInvalidWaitForCompletion() throws Exception {
        try {
            SubmitAsyncSearchRequest validRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            validRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
            AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(validRequest);
            assertNotNull(asyncSearchResponse.getSearchResponse());
            updateClusterSettings(AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(2));
            SubmitAsyncSearchRequest invalidRequest = new SubmitAsyncSearchRequest(new SearchRequest());
            invalidRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
            ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsyncSearch(invalidRequest));
            assertThat(responseException.getMessage(), containsString("Wait for completion timeout for async search (" +
                    validRequest.getWaitForCompletionTimeout().getMillis() + ") is too large"));
            updateClusterSettings(AsyncSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(60));
        } finally {
            deleteIndexIfExists();
        }

    }

    public void testMaxRunningAsyncSearchContexts() throws Exception {
        try {
            int numThreads = 50;
            List<Thread> threadsList = new LinkedList<>();
            CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
            for (int i = 0; i < numThreads; i++) {
                threadsList.add(new Thread(() -> {
                    try {
                        SubmitAsyncSearchRequest validRequest = new SubmitAsyncSearchRequest(new SearchRequest());
                        validRequest.keepAlive(TimeValue.timeValueDays(1));
                        AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(validRequest);
                        assertNotNull(asyncSearchResponse.getSearchResponse());
                    } catch (IOException e) {
                        fail("submit request failed");
                    } finally {
                        try {
                            barrier.await();
                        } catch (Exception e) {
                            fail();
                        }
                    }
                }
                ));
            }
            threadsList.forEach(Thread::start);
            barrier.await();
            for (Thread thread : threadsList) {
                thread.join();
            }

            updateClusterSettings(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), 0);
            threadsList.clear();
            AtomicInteger numFailures = new AtomicInteger();
            for (int i = 0; i < numThreads; i++) {
                threadsList.add(new Thread(() -> {
                    try {
                        SubmitAsyncSearchRequest validRequest = new SubmitAsyncSearchRequest(new SearchRequest());
                        validRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
                        AsyncSearchResponse asyncSearchResponse = executeSubmitAsyncSearch(validRequest);
                    } catch (Exception e) {
                        assertTrue(e instanceof ResponseException);
                        assertThat(e.getMessage(), containsString("Trying to create too many running contexts"));
                        numFailures.getAndIncrement();

                    } finally {
                        try {
                            barrier.await();
                        } catch (Exception e) {
                            fail();
                        }
                    }
                }
                ));
            }
            threadsList.forEach(Thread::start);
            barrier.await();
            for (Thread thread : threadsList) {
                thread.join();
            }
            assertEquals(numFailures.get(), 50);
            updateClusterSettings(AsyncSearchActiveStore.MAX_RUNNING_CONTEXT.getKey(), DEFAULT_MAX_RUNNING_CONTEXTS);
        } finally {
            deleteIndexIfExists();
        }
    }
}
