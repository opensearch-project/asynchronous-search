/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.listener;

import org.opensearch.search.asynchronous.utils.AsynchronousSearchAssertions;
import org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class AsynchronousSearchCancellationIT extends AsynchronousSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedBlockPlugin.class);
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

    private void cancelSearch(String action) {
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
        testCase(client(), searchRequest, plugins);
    }


    public void testCancellationDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        testCase(client(), searchRequest, plugins);
    }

    private void testCase(Client client, SearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        AtomicBoolean reduceContextInvocation = new AtomicBoolean();
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsynchronousSearchProgressListenerIT.class.getName());
            SearchService service = internalCluster().getInstance(SearchService.class);
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request);
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Function<SearchResponse, AsynchronousSearchResponse> responseFunction =
                    (r) -> null;
            Function<Exception, AsynchronousSearchResponse> failureFunction =
                    (e) -> null;
            AsynchronousSearchProgressListener listener = new AsynchronousSearchProgressListener(threadPool.relativeTimeInMillis(),
                    responseFunction,
                    failureFunction, threadPool.generic(), threadPool::relativeTimeInMillis,
                    () -> {
                        assertTrue(reduceContextInvocation.compareAndSet(false, true));
                        return reduceContextBuilder;
                    }) {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    assertTrue(responseRef.compareAndSet(null, searchResponse));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    assertTrue(exceptionRef.compareAndSet(null, exception));
                    latch.countDown();
                }
            };
            client.execute(SearchAction.INSTANCE, new SearchRequest(request) {
                @Override
                public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    SearchTask task = super.createTask(id, type, action, parentTaskId, headers);
                    task.setProgressListener(listener);
                    return task;
                }
            }, listener);

            awaitForBlock(plugins);
            cancelSearch(SearchAction.NAME);
            disableBlocks(plugins);

            latch.await();
            assertFalse(reduceContextInvocation.get());
            if (responseRef.get() != null) {
                AsynchronousSearchAssertions.assertSearchResponses(responseRef.get(), listener.partialResponse());
            } else {
                assertNotNull(exceptionRef.get());
                assertThat(exceptionRef.get(), instanceOf(SearchPhaseExecutionException.class));
            }
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
}
