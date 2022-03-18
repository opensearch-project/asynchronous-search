/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.integTests;

import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class AsynchronousSearchTaskCancellationIT extends AsynchronousSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ScriptedBlockPlugin.class, AsynchronousSearchPlugin.class);
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
                .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
                .build();
    }

    private void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test").setId(Integer.toString(i * 5 + j))
                        .setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private void cancelAsynchronousSearch(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        TaskInfo searchTask = listTasksResponse.getTasks().get(0);

        logger.info("Cancelling search");
        CancelTasksResponse cancelTasksResponse = client().admin().cluster().prepareCancelTasks().setTaskId(searchTask.getTaskId()).get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).getTaskId(), equalTo(searchTask.getTaskId()));
    }

    public void testCancellationDuringQueryPhase() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        SearchRequest searchRequest = client().prepareSearch("test").setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        //We need a NodeClient to make sure the listener gets injected in the search request execution.
        //Randomized client randomly return NodeClient/TransportClient
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(10000));
        testCase(internalCluster().smartClient(), submitAsynchronousSearchRequest, plugins);
    }


    public void testCancellationDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(false);
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(50000));
        testCase(internalCluster().smartClient(), submitAsynchronousSearchRequest, plugins);
    }

    private void testCase(Client client, SubmitAsynchronousSearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        final AtomicReference<SearchResponse> searchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client.execute(SubmitAsynchronousSearchAction.INSTANCE, request, new ActionListener<AsynchronousSearchResponse>() {
            @Override
            public void onResponse(AsynchronousSearchResponse asResponse) {
                searchResponseRef.set(asResponse.getSearchResponse());
                exceptionRef.set(asResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        awaitForBlock(plugins);
        cancelAsynchronousSearch(SubmitAsynchronousSearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        latch.await();
        ensureSearchWasCancelled(searchResponseRef.get(), exceptionRef.get());
    }
}
