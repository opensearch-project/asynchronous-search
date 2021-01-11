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

package com.amazon.opendistroforelasticsearch.search.async.request;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(numDataNodes = 5)
public class AsyncSearchRequestRoutingIT extends AsyncSearchIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    public void testRequestForwardingToCoordinatorNodeForPersistedAsyncSearch() throws Exception {
        String idx = "idx";
        assertAcked(prepareCreate(idx)
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        indexRandom(true,
                client().prepareIndex(idx, "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex(idx, "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex(idx, "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(idx));
        request.keepOnCompletion(true);
        AsyncSearchResponse submitResponse = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(submitResponse.getId());
        assertNotNull(submitResponse.getId());
        waitUntil(() -> TestClientUtils.blockingGetAsyncSearchResponse(client(), new GetAsyncSearchRequest(submitResponse.getId()))
                .getState().equals(AsyncSearchState.STORE_RESIDENT));
        assertNotNull(submitResponse.getSearchResponse());
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
        List<String> nonCoordinatorNodeNames = new LinkedList<>();
        clusterService.state().nodes().iterator().forEachRemaining(node -> {
            if (asyncSearchId.getNode().equals(node.getId()) == false)
                nonCoordinatorNodeNames.add(node.getName());
        });
        nonCoordinatorNodeNames.forEach(n -> {
            try {
                AsyncSearchResponse getResponse = client(n).execute(GetAsyncSearchAction.INSTANCE,
                        new GetAsyncSearchRequest(submitResponse.getId())).get();
                assertEquals(getResponse, new AsyncSearchResponse(submitResponse.getId(), AsyncSearchState.STORE_RESIDENT,
                        submitResponse.getStartTimeMillis(), submitResponse.getExpirationTimeMillis(), submitResponse.getSearchResponse(),
                        submitResponse.getError()));
            } catch (InterruptedException | ExecutionException e) {
                fail("Get async search request should not have failed");
            }
        });
    }

    public void testRequestForwardingToCoordinatorNodeForRunningAsyncSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        String index = "idx";
        assertAcked(prepareCreate(index)
                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(true,
                client().prepareIndex(index, "type", "1").setSource(
                        "ip", "192.168.1.7",
                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
                client().prepareIndex(index, "type", "2").setSource(
                        "ip", "192.168.1.10",
                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
                client().prepareIndex(index, "type", "3").setSource(
                        "ip", "2001:db8::ff00:42:8329",
                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME,
                        Collections.emptyMap())))
                .request();
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.keepOnCompletion(false);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(0));
        CountDownLatch latch = new CountDownLatch(1);

        client().execute(SubmitAsyncSearchAction.INSTANCE, request, new LatchedActionListener<>(new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse submitResponse) {
                String id = submitResponse.getId();
                assertNotNull(id);
                assertEquals(AsyncSearchState.RUNNING, submitResponse.getState());
                AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(id);
                ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
                assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
                List<String> nonCoordinatorNodeNames = new LinkedList<>();
                clusterService.state().nodes().iterator().forEachRemaining(node -> {
                    if (asyncSearchId.getNode().equals(node.getId()) == false)
                        nonCoordinatorNodeNames.add(node.getName());
                });
                nonCoordinatorNodeNames.forEach(n -> {
                    try {
                        AsyncSearchResponse getResponse = client(n).execute(GetAsyncSearchAction.INSTANCE,
                                new GetAsyncSearchRequest(id)).get();
                        assertEquals(getResponse.getState(), AsyncSearchState.RUNNING);
                    } catch (InterruptedException | ExecutionException e) {
                        fail("Get async search request should not have failed");
                    }
                });
                String randomNonCoordinatorNode = nonCoordinatorNodeNames.get(randomInt(nonCoordinatorNodeNames.size() - 1));
                try {
                    AcknowledgedResponse acknowledgedResponse =
                            client(randomNonCoordinatorNode).execute(DeleteAsyncSearchAction.INSTANCE,
                                    new DeleteAsyncSearchRequest(id)).get();
                    assertTrue(acknowledgedResponse.isAcknowledged());
                    ExecutionException executionException = expectThrows(ExecutionException.class,
                            () -> client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncSearchRequest(id)).get());
                    assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
                } catch (InterruptedException | ExecutionException e) {
                    fail("Delete async search request from random non-coordinator node should have succeeded.");
                }

            }

            @Override
            public void onFailure(Exception e) {
                fail(e.getMessage());
            }
        }, latch));
        latch.await();
        disableBlocks(plugins);
    }
//TODO
//    public void testCoordinatorNodeDropOnPersistedSearch() throws Exception {
//        String idx = "idx";
//        assertAcked(prepareCreate(idx)
//                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
//        waitForRelocation(ClusterHealthStatus.GREEN);
//        indexRandom(true,
//                client().prepareIndex(idx, "type", "1").setSource(
//                        "ip", "192.168.1.7",
//                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
//                client().prepareIndex(idx, "type", "2").setSource(
//                        "ip", "192.168.1.10",
//                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
//                client().prepareIndex(idx, "type", "3").setSource(
//                        "ip", "2001:db8::ff00:42:8329",
//                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));
//
//        assertAcked(prepareCreate("idx_unmapped"));
//        waitForRelocation(ClusterHealthStatus.GREEN);
//        refresh();
//        List<DiscoveryNode> dataNodes = new ArrayList<>();
//        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(entry -> dataNodes.add(entry.value));
//        assertFalse(dataNodes.isEmpty());
//        DiscoveryNode coordinatorNode = dataNodes.get(0);
//        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchRequest(idx));
//        request.keepOnCompletion(true);
//        AsyncSearchResponse submitResponse = client(coordinatorNode.getName()).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
//        TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
//        CountDownLatch latch = new CountDownLatch(1);
//        internalCluster().restartNode(coordinatorNode.getName(), new InternalTestCluster.RestartCallback() {
//            @Override
//            public Settings onNodeStopped(String nodeName) throws Exception {
//                try {
//                    AsyncSearchResponse getResponse = client(dataNodes.get(1).getName()).execute(GetAsyncSearchAction.INSTANCE,
//                            new GetAsyncSearchRequest(submitResponse.getId())).get();
//                    assertEquals(getResponse.getState(), AsyncSearchState.PERSISTED);
//                    assertEquals(getResponse.getId(), submitResponse.getId());
//                    return super.onNodeStopped(nodeName);
//                } finally {
//                    latch.countDown();
//                }
//            }
//        });
//        latch.await();
//
//    }
//TODO after test completes MockSearchService.assertNoInFlightContext() FAILS because we test killing a node with a running blocked search

//    public void testCoordinatorNodeDropOnRunningSearch() throws Exception {
//        List<ScriptedBlockPlugin> plugins = initBlockFactory();
//        String idx = "idx";
//        assertAcked(prepareCreate(idx)
//                .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
//        waitForRelocation(ClusterHealthStatus.GREEN);
//        indexRandom(true,
//                client().prepareIndex(idx, "type", "1").setSource(
//                        "ip", "192.168.1.7",
//                        "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
//                client().prepareIndex(idx, "type", "2").setSource(
//                        "ip", "192.168.1.10",
//                        "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
//                client().prepareIndex(idx, "type", "3").setSource(
//                        "ip", "2001:db8::ff00:42:8329",
//                        "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));
//
//        assertAcked(prepareCreate("idx_unmapped"));
//        waitForRelocation(ClusterHealthStatus.GREEN);
//        refresh();
//
//        List<DiscoveryNode> dataNodes = new ArrayList<>();
//        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(entry -> dataNodes.add(entry.value));
//        assertFalse(dataNodes.isEmpty());
//        DiscoveryNode coordinatorNode = dataNodes.get(0);
//        SearchRequest searchRequest = client().prepareSearch(idx).setQuery(
//                scriptQuery(new Script(
//                        ScriptType.INLINE, "mockscript", SCRIPT_NAME,
//                        Collections.emptyMap())))
//                .request();
//        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
//        request.keepOnCompletion(true);
//        AsyncSearchResponse submitResponse = client(coordinatorNode.getName()).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
//        CountDownLatch latch = new CountDownLatch(1);
//        internalCluster().restartNode(coordinatorNode.getName(), new InternalTestCluster.RestartCallback() {
//            @Override
//            public boolean clearData(String nodeName) {
//                return super.clearData(nodeName);
//            }
//
//            @Override
//            public Settings onNodeStopped(String nodeName) throws Exception {
//                try {
//                    ExecutionException executionException = expectThrows(ExecutionException.class,
//                            () -> client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncSearchRequest(submitResponse.getId()))
//                            .get());
//                    assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
//                    disableBlocks(plugins);
//                    return super.onNodeStopped(nodeName);
//                } finally {
//                    latch.countDown();
//                }
//            }
//        });
//        latch.await();
//        SearchService instance = internalCluster().getInstance(SearchService.class);
//        assertTrue(instance instanceof MockSearchService);
//    }

    public void testInvalidIdRequestHandling() {
        ExecutionException executionException = expectThrows(ExecutionException.class, () -> client().execute(GetAsyncSearchAction.INSTANCE,
                new GetAsyncSearchRequest(randomAlphaOfLength(16))).get());
        assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
    }
}
