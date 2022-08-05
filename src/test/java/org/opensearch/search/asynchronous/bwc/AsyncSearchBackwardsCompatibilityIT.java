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

package org.opensearch.search.asynchronous.bwc;

import org.junit.Assert;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.restIT.AsynchronousSearchRestTestCase;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.opensearch.search.asynchronous.settings.LegacyOpendistroAsynchronousSearchSettings;
import org.opensearch.search.asynchronous.utils.RestTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class AsyncSearchBackwardsCompatibilityIT extends AsynchronousSearchRestTestCase {
    private static final ClusterType CLUSTER_TYPE =
            ClusterType.parse(System.getProperty("tests.rest.bwcsuite_cluster"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

    public void testBackwardsCompatibility() throws Exception {
        String uri = getUri();
        Map<String, Map<String, Object>> responseMap =
                (Map<String, Map<String, Object>>) getAsMap(uri).get("nodes");
        for (Map<String, Object> response : responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
            Set<Object> pluginNames =
                    plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
            switch (CLUSTER_TYPE) {
                case OLD:
                case MIXED:
                    Assert.assertTrue(pluginNames.contains("opensearch-asynchronous-search"));
                    testAsyncSearchAndSettingsApi(true);
                    break;
                case UPGRADED:
                    Assert.assertTrue(pluginNames.contains("opensearch-asynchronous-search"));
                    testAsyncSearchAndSettingsApi(false);
                    break;
            }
            break;
        }
    }

    private void testAsyncSearchAndSettingsApi(boolean shouldUseLegacyApi) throws Exception {
        testSubmitWithRetainedResponse(shouldUseLegacyApi);
        testMaxKeepAliveSetting(shouldUseLegacyApi);
        testSubmitInvalidWaitForCompletion(shouldUseLegacyApi);
        testMaxRunningAsynchronousSearchContexts(shouldUseLegacyApi);
        testStoreAsyncSearchWithFailures(shouldUseLegacyApi);
    }

    private String getUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED:
                String round = System.getProperty("tests.rest.bwcsuite_round");
                if (round.equals("second")) {
                    return "_nodes/" + CLUSTER_NAME + "-1/plugins";
                } else if (round.equals("third")) {
                    return "_nodes/" + CLUSTER_NAME + "-2/plugins";
                } else {
                    return "_nodes/" + CLUSTER_NAME + "-0/plugins";
                }
            case UPGRADED:
                return "_nodes/plugins";
            default:
                throw new AssertionError("unknown cluster type: " + CLUSTER_TYPE);
        }
    }

    public void testSubmitWithRetainedResponse(boolean shouldUseLegacyApi) throws IOException {
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest =
                new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(
                TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse =
                executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest, shouldUseLegacyApi);
        List<AsynchronousSearchState> legalStates =
                Arrays.asList(
                        AsynchronousSearchState.RUNNING,
                        AsynchronousSearchState.SUCCEEDED,
                        AsynchronousSearchState.PERSIST_SUCCEEDED,
                        AsynchronousSearchState.PERSISTING,
                        AsynchronousSearchState.CLOSED,
                        AsynchronousSearchState.STORE_RESIDENT);
        assertNotNull(submitResponse.getId());
        assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
        GetAsynchronousSearchRequest getAsynchronousSearchRequest =
                new GetAsynchronousSearchRequest(submitResponse.getId());
        AsynchronousSearchResponse getResponse;
        do {
            getResponse =
                    getAssertedAsynchronousSearchResponse(
                            submitResponse, getAsynchronousSearchRequest, shouldUseLegacyApi);
            if (getResponse.getState() == AsynchronousSearchState.RUNNING
                    && getResponse.getSearchResponse() != null) {
                assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 0);
            } else {
                assertNotNull(getResponse.getSearchResponse());
                assertNotEquals(getResponse.getSearchResponse().getTook(), -1L);
            }
        } while (AsynchronousSearchState.STORE_RESIDENT.equals(getResponse.getState()) == false);
        getResponse =
                getAssertedAsynchronousSearchResponse(
                        submitResponse, getAsynchronousSearchRequest, shouldUseLegacyApi);
        assertNotNull(getResponse.getSearchResponse());
        assertEquals(AsynchronousSearchState.STORE_RESIDENT, getResponse.getState());
        assertHitCount(getResponse.getSearchResponse(), 5);
        executeDeleteAsynchronousSearch(
                new DeleteAsynchronousSearchRequest(submitResponse.getId()), shouldUseLegacyApi);
    }

    Response executeDeleteAsynchronousSearch(
            DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest, boolean shouldUseLegacyApi)
            throws IOException {
        Request request = RestTestUtils.buildHttpRequest(deleteAsynchronousSearchRequest, shouldUseLegacyApi);
        return client().performRequest(request);
    }

    public void testMaxKeepAliveSetting(boolean shouldUseLegacyApi) throws Exception {
        SubmitAsynchronousSearchRequest validRequest =
                new SubmitAsynchronousSearchRequest(new SearchRequest());
        validRequest.keepAlive(TimeValue.timeValueHours(7));
        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest, shouldUseLegacyApi);
        assertNotNull(asResponse.getSearchResponse());
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING.getKey()
                        : AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.getKey(),
                TimeValue.timeValueHours(6));
        SubmitAsynchronousSearchRequest invalidRequest =
                new SubmitAsynchronousSearchRequest(new SearchRequest());
        invalidRequest.keepAlive(TimeValue.timeValueHours(7));
        ResponseException responseException =
                expectThrows(
                        ResponseException.class,
                        () -> executeSubmitAsynchronousSearch(invalidRequest, shouldUseLegacyApi));
        assertThat(
                responseException.getMessage(),
                containsString(
                        "Keep alive for asynchronous search ("
                                + invalidRequest.getKeepAlive().getMillis()
                                + ") is too large"));
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.MAX_KEEP_ALIVE_SETTING.getKey()
                        : AsynchronousSearchService.MAX_KEEP_ALIVE_SETTING.getKey(),
                TimeValue.timeValueHours(24));
    }

    public void testSubmitInvalidWaitForCompletion(boolean shouldUseLegacyApi) throws Exception {
        SubmitAsynchronousSearchRequest validRequest =
                new SubmitAsynchronousSearchRequest(new SearchRequest());
        validRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(validRequest, shouldUseLegacyApi);
        assertNotNull(asResponse.getSearchResponse());
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING
                        .getKey()
                        : AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueSeconds(2));
        SubmitAsynchronousSearchRequest invalidRequest =
                new SubmitAsynchronousSearchRequest(new SearchRequest());
        invalidRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(50));
        ResponseException responseException =
                expectThrows(
                        ResponseException.class,
                        () -> executeSubmitAsynchronousSearch(invalidRequest, shouldUseLegacyApi));
        assertThat(
                responseException.getMessage(),
                containsString(
                        "Wait for completion timeout for asynchronous search ("
                                + validRequest.getWaitForCompletionTimeout().getMillis()
                                + ") is too large"));
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING
                        .getKey()
                        : AsynchronousSearchService.MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueSeconds(60));
    }

    public void testMaxRunningAsynchronousSearchContexts(boolean shouldUseLegacyApi) throws Exception {
        int numThreads = 50;
        List<Thread> threadsList = new LinkedList<>();
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < numThreads; i++) {
            threadsList.add(
                    new Thread(
                            () -> {
                                try {
                                    SubmitAsynchronousSearchRequest validRequest =
                                            new SubmitAsynchronousSearchRequest(new SearchRequest());
                                    validRequest.keepAlive(TimeValue.timeValueHours(1));
                                    AsynchronousSearchResponse asResponse =
                                            executeSubmitAsynchronousSearch(validRequest, shouldUseLegacyApi);
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
                            }));
        }
        threadsList.forEach(Thread::start);
        barrier.await();
        for (Thread thread : threadsList) {
            thread.join();
        }

        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING
                        .getKey()
                        : AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(),
                0);
        threadsList.clear();
        AtomicInteger numFailures = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            threadsList.add(
                    new Thread(
                            () -> {
                                try {
                                    SubmitAsynchronousSearchRequest validRequest =
                                            new SubmitAsynchronousSearchRequest(new SearchRequest());
                                    validRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
                                    AsynchronousSearchResponse asResponse =
                                            executeSubmitAsynchronousSearch(validRequest, shouldUseLegacyApi);
                                } catch (Exception e) {
                                    assertTrue(e instanceof ResponseException);
                                    assertThat(
                                            e.getMessage(),
                                            containsString("Trying to create too many concurrent searches"));
                                    numFailures.getAndIncrement();

                                } finally {
                                    try {
                                        barrier.await();
                                    } catch (Exception e) {
                                        fail();
                                    }
                                }
                            }));
        }
        threadsList.forEach(Thread::start);
        barrier.await();
        for (Thread thread : threadsList) {
            thread.join();
        }
        assertEquals(numFailures.get(), 50);
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING
                        .getKey()
                        : AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(),
                AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES);
    }

    public void testStoreAsyncSearchWithFailures(boolean shouldUseLegacyApi) throws Exception {
        SubmitAsynchronousSearchRequest request =
                new SubmitAsynchronousSearchRequest(new SearchRequest("non-existent-index"));
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMinutes(1));
        AsynchronousSearchResponse response = executeSubmitAsynchronousSearch(request, shouldUseLegacyApi);
        assertTrue(
                Arrays.asList(AsynchronousSearchState.CLOSED, AsynchronousSearchState.FAILED)
                        .contains(AsynchronousSearchState.FAILED));
        waitUntil(
                () -> {
                    try {
                        executeGetAsynchronousSearch(
                                new GetAsynchronousSearchRequest(response.getId()), shouldUseLegacyApi);
                        return false;
                    } catch (IOException e) {
                        return e.getMessage().contains("resource_not_found");
                    }
                });
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING.getKey()
                        : AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(),
                true);
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(request, shouldUseLegacyApi);
        waitUntil(
                () -> {
                    try {
                        return executeGetAsynchronousSearch(
                                new GetAsynchronousSearchRequest(submitResponse.getId()), shouldUseLegacyApi)
                                .getState()
                                .equals(AsynchronousSearchState.STORE_RESIDENT);
                    } catch (IOException e) {
                        return false;
                    }
                });
        assertEquals(
                executeGetAsynchronousSearch(
                        new GetAsynchronousSearchRequest(submitResponse.getId()), shouldUseLegacyApi)
                        .getState(),
                AsynchronousSearchState.STORE_RESIDENT);
        updateClusterSettings(
                shouldUseLegacyApi
                        ? LegacyOpendistroAsynchronousSearchSettings.PERSIST_SEARCH_FAILURES_SETTING.getKey()
                        : AsynchronousSearchService.PERSIST_SEARCH_FAILURES_SETTING.getKey(),
                false);
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new IllegalArgumentException("unknown cluster type: " + value);
            }
        }
    }
}