/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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

package org.opensearch.search.asynchronous.restIT;

import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.ResponseException;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;

public class AsynchronousSearchSettingsIT extends AsynchronousSearchRestTestCase {

    public void testMaxKeepAliveSetting() throws Exception {
        SubmitAsynchronousSearchRequest validRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
        validRequest.keepAlive(TimeValue.timeValueHours(7));
        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest);
        assertNotNull(asResponse.getSearchResponse());
        updateClusterSettings(AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.getKey(), TimeValue.timeValueHours(6));
        SubmitAsynchronousSearchRequest invalidRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
        invalidRequest.keepAlive(TimeValue.timeValueHours(7));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(
                invalidRequest));
        assertThat(responseException.getMessage(), containsString("Keep alive for asynchronous search (" +
                invalidRequest.getKeepAlive().getMillis() + ") is too large"));
        updateClusterSettings(AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.getKey(), TimeValue.timeValueHours(24));
    }

    public void testSubmitInvalidWaitForCompletion() throws Exception {
        SubmitAsynchronousSearchRequest validRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
        validRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest);
        assertNotNull(asResponse.getSearchResponse());
        updateClusterSettings(AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueSeconds(2));
        SubmitAsynchronousSearchRequest invalidRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
        invalidRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
        ResponseException responseException = expectThrows(ResponseException.class, () -> executeSubmitAsynchronousSearch(
                invalidRequest));
        assertThat(responseException.getMessage(), containsString("Wait for completion timeout for asynchronous search (" +
                validRequest.getWaitForCompletionTimeout().getMillis() + ") is too large"));
        updateClusterSettings(AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueSeconds(60));
    }

    public void testMaxRunningAsynchronousSearchContexts() throws Exception {
        int numThreads = 50;
        List<Thread> threadsList = new LinkedList<>();
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < numThreads; i++) {
            threadsList.add(new Thread(() -> {
                try {
                    SubmitAsynchronousSearchRequest validRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
                    validRequest.keepAlive(TimeValue.timeValueHours(1));
                    AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest);
                    assertNotNull(asResponse.getSearchResponse());
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

        updateClusterSettings(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), 0);
        threadsList.clear();
        AtomicInteger numFailures = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            threadsList.add(new Thread(() -> {
                try {
                    SubmitAsynchronousSearchRequest validRequest = new SubmitAsynchronousSearchRequest(new SearchRequest());
                    validRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
                    AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest);
                } catch (Exception e) {
                    assertTrue(e instanceof ResponseException);
                    assertThat(e.getMessage(), containsString("Trying to create too many concurrent searches"));
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
        updateClusterSettings(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(),
                AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES);
    }

    public void testStoreAsyncSearchWithFailures() throws Exception {
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest("non-existent-index"));
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMinutes(1));
        AsynchronousSearchResponse response = executeSubmitAsynchronousSearch(request);
        assertTrue(Arrays.asList(AsynchronousSearchState.CLOSED, AsynchronousSearchState.FAILED).contains(AsynchronousSearchState.FAILED));
        waitUntil(() -> {
            try {
                executeGetAsynchronousSearch(new GetAsynchronousSearchRequest(response.getId()));
                return false;
            } catch (IOException e) {
                return e.getMessage().contains("resource_not_found");
            }
        });
        updateClusterSettings(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(),
                true);
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(request);
        waitUntil(() -> {
            try {
                return executeGetAsynchronousSearch(new GetAsynchronousSearchRequest(submitResponse.getId())).getState()
                        .equals(AsynchronousSearchState.STORE_RESIDENT);
            } catch (IOException e) {
                return false;
            }
        });
        assertEquals(executeGetAsynchronousSearch(new GetAsynchronousSearchRequest(submitResponse.getId())).getState(),
                AsynchronousSearchState.STORE_RESIDENT);
        updateClusterSettings(AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(),
                false);
    }
}
