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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchAssertions;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchCancellationIT.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class AsyncSearchCancellationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedBlockPlugin.class);
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
                bulkRequestBuilder.add(client().prepareIndex("test", "type", Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
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
        testCase(internalCluster().smartClient(), searchRequest, plugins);
    }


    public void testCancellationDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        testCase(internalCluster().smartClient(), searchRequest, plugins);
    }

    private void testCase(Client client, SearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        AtomicBoolean reduceContextInvocation = new AtomicBoolean();
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsyncSearchProgressListenerIT.class.getName());
            SearchService service = internalCluster().getInstance(SearchService.class);
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = service.aggReduceContextBuilder(request);
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            Function<SearchResponse, AsyncSearchResponse> responseFunction =
                    (r) -> null;
            Function<Exception, AsyncSearchResponse> failureFunction =
                    (e) -> null;
            AsyncSearchProgressListener listener = new AsyncSearchProgressListener(threadPool.relativeTimeInMillis(), responseFunction,
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
                AsyncSearchAssertions.assertSearchResponses(responseRef.get(), listener.partialResponse());
            } else {
                assertNotNull(exceptionRef.get());
                assertThat(exceptionRef.get(), instanceOf(SearchPhaseExecutionException.class));
            }
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(AsyncSearchCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}