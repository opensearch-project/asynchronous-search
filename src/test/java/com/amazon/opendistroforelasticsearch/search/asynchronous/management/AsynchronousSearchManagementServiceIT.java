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

package com.amazon.opendistroforelasticsearch.search.asynchronous.management;

import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.GetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.task.AsynchronousSearchTask;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.reindex.ReindexPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class AsynchronousSearchManagementServiceIT extends AsynchronousSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ScriptedBlockPlugin.class,
                AsynchronousSearchPlugin.class,
                ReindexPlugin.class);
    }

    //We need to apply blocks via ScriptedBlockPlugin, external clusters are immutable
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.attr.asynchronous_search_enabled", true)
                .put(AsynchronousSearchManagementService.ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING.getKey(),  TimeValue.timeValueSeconds(5))
                .put(AsynchronousSearchManagementService.PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING.getKey(),
                        TimeValue.timeValueSeconds(5))
                .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
                .build();
    }

    private void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test", "type", Integer.toString(i * 5 + j))
                        .setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    public void testCleansUpExpiredAsynchronousSearchDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        SearchRequest searchRequest = client().prepareSearch("test").setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        //We need a NodeClient to make sure the listener gets injected in the search request execution.
        //Randomized client randomly return NodeClient/TransportClient
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsynchronousSearchRequest, plugins);
    }


    public void testCleansUpExpiredAsynchronousSearchDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsynchronousSearchRequest, plugins);
    }

    public void testDeletesExpiredAsynchronousSearchResponseFromPersistedStore() throws Exception {
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

        final AtomicReference<AsynchronousSearchResponse> asResponseRef = new AtomicReference<>();
        final AtomicReference<AsynchronousSearchResponse> nonExpiredAsynchronousSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        SearchRequest searchRequest = new SearchRequest(idx);
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        CountDownLatch latch = new CountDownLatch(2);
        client().execute(SubmitAsynchronousSearchAction.INSTANCE, submitAsynchronousSearchRequest,
                new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                asResponseRef.set(asResponse);
                exceptionRef.set(asResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        //submit another request to verify that the second request is not cancelled
        client().execute(SubmitAsynchronousSearchAction.INSTANCE, submitAsynchronousSearchRequest,
                new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                nonExpiredAsynchronousSearchResponseRef.set(asResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        latch.await();
        waitUntil(() -> verifyResponsePersisted(asResponseRef.get().getId()));
        waitUntil(() -> verifyResponsePersisted(nonExpiredAsynchronousSearchResponseRef.get().getId()));
        CountDownLatch updateLatch = new CountDownLatch(1);
        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(asResponseRef.get().getId());
        getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueMillis(1));
        client().execute(GetAsynchronousSearchAction.INSTANCE, getAsynchronousSearchRequest,
                new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                asResponseRef.set(asResponse);
                exceptionRef.set(asResponse.getError());
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                updateLatch.countDown();
            }
        });
        updateLatch.await();
        waitUntil(() -> verifyResponseRemoved(asResponseRef.get().getId()));
        assertBusy(() -> assertTrue(verifyResponsePersisted(nonExpiredAsynchronousSearchResponseRef.get().getId())));
        // delete the non expired response explicitly
        CountDownLatch deleteLatch = new CountDownLatch(1);
        DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(
                nonExpiredAsynchronousSearchResponseRef.get().getId());
        client().execute(DeleteAsynchronousSearchAction.INSTANCE, deleteAsynchronousSearchRequest,
                new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                assertTrue(response.isAcknowledged());
                deleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                deleteLatch.countDown();
                fail("Cleanup failed");
            }
        });
        deleteLatch.await();
    }

    private void testCase(Client client, SubmitAsynchronousSearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        final AtomicReference<AsynchronousSearchResponse> asResponseRef = new AtomicReference<>();
        final AtomicReference<AsynchronousSearchResponse> nonExpiredAsynchronousSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);
        client.execute(SubmitAsynchronousSearchAction.INSTANCE, request, new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                asResponseRef.set(asResponse);
                exceptionRef.set(asResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        //submit another request to verify that the second request is not cancelled
        client.execute(SubmitAsynchronousSearchAction.INSTANCE, request, new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                nonExpiredAsynchronousSearchResponseRef.set(asResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });
        latch.await();
        awaitForBlock(plugins);
        assertNotNull(asResponseRef.get());
        CountDownLatch updateLatch = new CountDownLatch(1);
        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(asResponseRef.get().getId());
        getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueMillis(1));
        client.execute(GetAsynchronousSearchAction.INSTANCE, getAsynchronousSearchRequest,
                new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                asResponseRef.set(asResponse);
                exceptionRef.set(asResponse.getError());
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                updateLatch.countDown();
            }
        });
        updateLatch.await();
        assertThat(asResponseRef.get().getExpirationTimeMillis(),
                lessThan((System.currentTimeMillis()) + randomLongBetween(100, 200)));
        boolean cleanedUp = waitUntil(() -> verifyAsynchronousSearchDoesNotExists(asResponseRef.get().getId()));
        assertTrue(cleanedUp);
        disableBlocks(plugins);
        AsynchronousSearchId asId = AsynchronousSearchIdConverter.parseAsyncId(asResponseRef.get().getId());
        TaskId taskId = new TaskId(asId.getNode(), asId.getTaskId());
        waitUntil(() -> verifyTaskCancelled(AsynchronousSearchTask.NAME, taskId));
        //ensure the second asynchronous search is not cleaned up
        assertBusy(() -> assertFalse(verifyAsynchronousSearchDoesNotExists(nonExpiredAsynchronousSearchResponseRef.get().getId())));
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        CountDownLatch deleteLatch = new CountDownLatch(1);
        //explicitly clean up the second request
        DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest = new DeleteAsynchronousSearchRequest(
                nonExpiredAsynchronousSearchResponseRef.get().getId());
        client.execute(DeleteAsynchronousSearchAction.INSTANCE, deleteAsynchronousSearchRequest,
                new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                assertTrue(response.isAcknowledged());
                deleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                deleteLatch.countDown();
                fail("Cleanup failed");
            }
        });
        deleteLatch.await();
    }
}
