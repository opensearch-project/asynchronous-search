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
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.restIT.AsynchronousSearchRestTestCase;
import org.opensearch.search.asynchronous.utils.RestTestUtils;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AsyncSearchBackwardsCompatibilityIT  extends AsynchronousSearchRestTestCase {

    public static final String KNN_BWC_PREFIX = "knn-bwc-";
    public static final String OS_KNN = "opensearch-knn";
    public static final String OPENDISTRO_SECURITY = ".opendistro_security";
    public static final String BWCSUITE_CLUSTER = "tests.rest.bwcsuite_cluster";
    public static final String BWCSUITE_ROUND = "tests.rest.bwcsuite_round";
    public static final String TEST_CLUSTER_NAME = "tests.clustername";
    private static final ClusterType CLUSTER_TYPE =
            ClusterType.parse(System.getProperty("tests.rest.bwcsuite_cluster"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

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
                    Assert.assertTrue(pluginNames.contains("opendistro-asynchronous-search"));
                    testSubmitWithRetainedResponse(true);
                    break;
                case MIXED:
                case UPGRADED:
                    Assert.assertTrue(pluginNames.contains("opensearch-asynchronous-search"));
                    testSubmitWithRetainedResponse(false);
                    break;
            }
            break;
        }
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

    private ClusterType getClusterType(){
        return ClusterType.parse(System.getProperty(BWCSUITE_CLUSTER));
    }

    public void testSubmitWithRetainedResponse(boolean isLegacy) throws IOException {
        SearchRequest searchRequest = new SearchRequest("test");
        searchRequest.source(new SearchSourceBuilder());
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(randomLongBetween(1, 500)));
        AsynchronousSearchResponse submitResponse = executeSubmitAsynchronousSearch(submitAsynchronousSearchRequest, isLegacy);
        List<AsynchronousSearchState> legalStates = Arrays.asList(
                AsynchronousSearchState.RUNNING, AsynchronousSearchState.SUCCEEDED, AsynchronousSearchState.PERSIST_SUCCEEDED,
                AsynchronousSearchState.PERSISTING,
                AsynchronousSearchState.CLOSED, AsynchronousSearchState.STORE_RESIDENT);
        assertNotNull(submitResponse.getId());
        assertTrue(submitResponse.getState().name(), legalStates.contains(submitResponse.getState()));
        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(submitResponse.getId());
        AsynchronousSearchResponse getResponse;
        do {
            getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest, isLegacy);
            if (getResponse.getState() == AsynchronousSearchState.RUNNING && getResponse.getSearchResponse() != null) {
                assertEquals(getResponse.getSearchResponse().getHits().getHits().length, 0);
            } else {
                assertNotNull(getResponse.getSearchResponse());
                assertNotEquals(getResponse.getSearchResponse().getTook(), -1L);
            }
        } while (AsynchronousSearchState.STORE_RESIDENT.equals(getResponse.getState()) == false);
        getResponse = getAssertedAsynchronousSearchResponse(submitResponse, getAsynchronousSearchRequest, isLegacy);
        assertNotNull(getResponse.getSearchResponse());
        assertEquals(AsynchronousSearchState.STORE_RESIDENT, getResponse.getState());
        assertHitCount(getResponse.getSearchResponse(), 5);
        executeDeleteAsynchronousSearch(new DeleteAsynchronousSearchRequest(submitResponse.getId()), isLegacy);
    }


    AsynchronousSearchResponse executeSubmitAsynchronousSearch(@Nullable SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest, boolean legacy)
            throws IOException {
        Request request = RestTestUtils.buildHttpRequest(submitAsynchronousSearchRequest, legacy);
        Response resp = client().performRequest(request);
        return parseEntity(resp.getEntity(), AsynchronousSearchResponse::fromXContent);
    }

    Response executeDeleteAsynchronousSearch(DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest, boolean legacy) throws IOException {
        Request request = RestTestUtils.buildHttpRequest(deleteAsynchronousSearchRequest, legacy);
        return client().performRequest(request);
    }
}
