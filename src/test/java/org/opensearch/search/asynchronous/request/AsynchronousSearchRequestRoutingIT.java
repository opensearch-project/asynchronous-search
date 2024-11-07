/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.request;

import org.opensearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.GetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.utils.TestClientUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 5)
public class AsynchronousSearchRequestRoutingIT extends AsynchronousSearchIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    public void testRequestForwardingToCoordinatorNodeForPersistedAsynchronousSearch() throws Exception {
        String idx = "idx";
        assertAcked(prepareCreate(idx).setMapping("ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        indexRandom(
            true,
            client().prepareIndex(idx).setId("1").setSource("ip", "192.168.1.7", "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
            client().prepareIndex(idx).setId("2").setSource("ip", "192.168.1.10", "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
            client().prepareIndex(idx)
                .setId("3")
                .setSource("ip", "2001:db8::ff00:42:8329", "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380"))
        );

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();

        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest(idx));
        request.keepOnCompletion(true);
        AsynchronousSearchResponse submitResponse = client().execute(SubmitAsynchronousSearchAction.INSTANCE, request).get();
        AsynchronousSearchId asId = AsynchronousSearchIdConverter.parseAsyncId(submitResponse.getId());
        assertNotNull(submitResponse.getId());
        waitUntil(
            () -> TestClientUtils.blockingGetAsynchronousSearchResponse(client(), new GetAsynchronousSearchRequest(submitResponse.getId()))
                .getState()
                .equals(AsynchronousSearchState.STORE_RESIDENT)
        );
        assertNotNull(submitResponse.getSearchResponse());
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
        List<String> nonCoordinatorNodeNames = new LinkedList<>();
        clusterService.state().nodes().iterator().forEachRemaining(node -> {
            if (asId.getNode().equals(node.getId()) == false) nonCoordinatorNodeNames.add(node.getName());
        });
        nonCoordinatorNodeNames.forEach(n -> {
            try {
                AsynchronousSearchResponse getResponse = client(n).execute(
                    GetAsynchronousSearchAction.INSTANCE,
                    new GetAsynchronousSearchRequest(submitResponse.getId())
                ).get();
                assertEquals(
                    getResponse,
                    new AsynchronousSearchResponse(
                        submitResponse.getId(),
                        AsynchronousSearchState.STORE_RESIDENT,
                        submitResponse.getStartTimeMillis(),
                        submitResponse.getExpirationTimeMillis(),
                        submitResponse.getSearchResponse(),
                        submitResponse.getError()
                    )
                );
            } catch (InterruptedException | ExecutionException e) {
                fail("Get asynchronous search request should not have failed");
            }
        });
    }

    public void testRequestForwardingToCoordinatorNodeForRunningAsynchronousSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        String index = "idx";
        assertAcked(prepareCreate(index).setMapping("ip", "type=ip", "ips", "type=ip"));
        waitForRelocation(ClusterHealthStatus.GREEN);

        indexRandom(
            true,
            client().prepareIndex(index).setId("1").setSource("ip", "192.168.1.7", "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
            client().prepareIndex(index).setId("2").setSource("ip", "192.168.1.10", "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
            client().prepareIndex(index)
                .setId("3")
                .setSource("ip", "2001:db8::ff00:42:8329", "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380"))
        );

        assertAcked(prepareCreate("idx_unmapped"));
        waitForRelocation(ClusterHealthStatus.GREEN);
        refresh();
        SearchRequest searchRequest = client().prepareSearch(index)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .request();
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.keepOnCompletion(false);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(0));
        CountDownLatch latch = new CountDownLatch(1);

        client().execute(
            SubmitAsynchronousSearchAction.INSTANCE,
            request,
            new LatchedActionListener<>(new ActionListener<AsynchronousSearchResponse>() {
                @Override
                public void onResponse(AsynchronousSearchResponse submitResponse) {
                    String id = submitResponse.getId();
                    assertNotNull(id);
                    assertEquals(AsynchronousSearchState.RUNNING, submitResponse.getState());
                    AsynchronousSearchId asId = AsynchronousSearchIdConverter.parseAsyncId(id);
                    ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
                    assertEquals(clusterService.state().nodes().getDataNodes().size(), 5);
                    List<String> nonCoordinatorNodeNames = new LinkedList<>();
                    clusterService.state().nodes().iterator().forEachRemaining(node -> {
                        if (asId.getNode().equals(node.getId()) == false) nonCoordinatorNodeNames.add(node.getName());
                    });
                    nonCoordinatorNodeNames.forEach(n -> {
                        try {
                            AsynchronousSearchResponse getResponse = client(n).execute(
                                GetAsynchronousSearchAction.INSTANCE,
                                new GetAsynchronousSearchRequest(id)
                            ).get();
                            assertEquals(getResponse.getState(), AsynchronousSearchState.RUNNING);
                        } catch (InterruptedException | ExecutionException e) {
                            fail("Get asynchronous search request should not have failed");
                        }
                    });
                    String randomNonCoordinatorNode = nonCoordinatorNodeNames.get(randomInt(nonCoordinatorNodeNames.size() - 1));
                    try {
                        AcknowledgedResponse acknowledgedResponse = client(randomNonCoordinatorNode).execute(
                            DeleteAsynchronousSearchAction.INSTANCE,
                            new DeleteAsynchronousSearchRequest(id)
                        ).get();
                        assertTrue(acknowledgedResponse.isAcknowledged());
                        ExecutionException executionException = expectThrows(
                            ExecutionException.class,
                            () -> client().execute(GetAsynchronousSearchAction.INSTANCE, new GetAsynchronousSearchRequest(id)).get()
                        );
                        assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
                    } catch (InterruptedException | ExecutionException e) {
                        fail("Delete asynchronous search request from random non-coordinator node should have succeeded.");
                    }

                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.getMessage());
                }
            }, latch)
        );
        latch.await();
        disableBlocks(plugins);
    }
    // TODO
    // public void testCoordinatorNodeDropOnPersistedSearch() throws Exception {
    // String idx = "idx";
    // assertAcked(prepareCreate(idx)
    // .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
    // waitForRelocation(ClusterHealthStatus.GREEN);
    // indexRandom(true,
    // client().prepareIndex(idx).setId("1").setSource(
    // "ip", "192.168.1.7",
    // "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
    // client().prepareIndex(idx).setId("2").setSource(
    // "ip", "192.168.1.10",
    // "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
    // client().prepareIndex(idx).setId("3").setSource(
    // "ip", "2001:db8::ff00:42:8329",
    // "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));
    //
    // assertAcked(prepareCreate("idx_unmapped"));
    // waitForRelocation(ClusterHealthStatus.GREEN);
    // refresh();
    // List<DiscoveryNode> dataNodes = new ArrayList<>();
    // clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(entry -> dataNodes.add(entry.value));
    // assertFalse(dataNodes.isEmpty());
    // DiscoveryNode coordinatorNode = dataNodes.get(0);
    // SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest(idx));
    // request.keepOnCompletion(true);
    // AsynchronousSearchResponse submitResponse = client(coordinatorNode.getName()).execute(SubmitAsynchronousSearchAction.INSTANCE,
    // request).get();
    // TestClientUtils.assertResponsePersistence(client(), submitResponse.getId());
    // CountDownLatch latch = new CountDownLatch(1);
    // internalCluster().restartNode(coordinatorNode.getName(), new InternalTestCluster.RestartCallback() {
    // @Override
    // public Settings onNodeStopped(String nodeName) throws Exception {
    // try {
    // AsynchronousSearchResponse getResponse = client(dataNodes.get(1).getName())
    // .execute(GetAsynchronousSearchAction.INSTANCE,
    // new GetAsynchronousSearchRequest(submitResponse.getId())).get();
    // assertEquals(getResponse.getState(), AsynchronousSearchState.PERSISTED);
    // assertEquals(getResponse.getId(), submitResponse.getId());
    // return super.onNodeStopped(nodeName);
    // } finally {
    // latch.countDown();
    // }
    // }
    // });
    // latch.await();
    //
    // }
    // TODO after test completes MockSearchService.assertNoInFlightContext() FAILS because we test killing a node with a running blocked
    // search

    // public void testCoordinatorNodeDropOnRunningSearch() throws Exception {
    // List<ScriptedBlockPlugin> plugins = initBlockFactory();
    // String idx = "idx";
    // assertAcked(prepareCreate(idx)
    // .addMapping("type", "ip", "type=ip", "ips", "type=ip"));
    // waitForRelocation(ClusterHealthStatus.GREEN);
    // indexRandom(true,
    // client().prepareIndex(idx).setId("1").setSource(
    // "ip", "192.168.1.7",
    // "ips", Arrays.asList("192.168.0.13", "192.168.1.2")),
    // client().prepareIndex(idx).setId("2").setSource(
    // "ip", "192.168.1.10",
    // "ips", Arrays.asList("192.168.1.25", "192.168.1.28")),
    // client().prepareIndex(idx).setId("3").setSource(
    // "ip", "2001:db8::ff00:42:8329",
    // "ips", Arrays.asList("2001:db8::ff00:42:8329", "2001:db8::ff00:42:8380")));
    //
    // assertAcked(prepareCreate("idx_unmapped"));
    // waitForRelocation(ClusterHealthStatus.GREEN);
    // refresh();
    //
    // List<DiscoveryNode> dataNodes = new ArrayList<>();
    // clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(entry -> dataNodes.add(entry.value));
    // assertFalse(dataNodes.isEmpty());
    // DiscoveryNode coordinatorNode = dataNodes.get(0);
    // SearchRequest searchRequest = client().prepareSearch(idx).setQuery(
    // scriptQuery(new Script(
    // ScriptType.INLINE, "mockscript", SCRIPT_NAME,
    // Collections.emptyMap())))
    // .request();
    // SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
    // request.keepOnCompletion(true);
    // AsynchronousSearchResponse submitResponse = client(coordinatorNode.getName()).execute(SubmitAsynchronousSearchAction.INSTANCE,
    // request).get();
    // CountDownLatch latch = new CountDownLatch(1);
    // internalCluster().restartNode(coordinatorNode.getName(), new InternalTestCluster.RestartCallback() {
    // @Override
    // public boolean clearData(String nodeName) {
    // return super.clearData(nodeName);
    // }
    //
    // @Override
    // public Settings onNodeStopped(String nodeName) throws Exception {
    // try {
    // ExecutionException executionException = expectThrows(ExecutionException.class,
    // () -> client().execute(GetAsynchronousSearchAction.INSTANCE, new GetAsynchronousSearchRequest(
    // submitResponse.getId()))
    // .get());
    // assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
    // disableBlocks(plugins);
    // return super.onNodeStopped(nodeName);
    // } finally {
    // latch.countDown();
    // }
    // }
    // });
    // latch.await();
    // SearchService instance = internalCluster().getInstance(SearchService.class);
    // assertTrue(instance instanceof MockSearchService);
    // }

    public void testInvalidIdRequestHandling() {
        ExecutionException executionException = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetAsynchronousSearchAction.INSTANCE, new GetAsynchronousSearchRequest(randomAlphaOfLength(16))).get()
        );
        assertThat(executionException.getMessage(), containsString("ResourceNotFoundException"));
    }
}
