package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class AsyncSearchManagementServiceIT extends AsyncSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ScriptedBlockPlugin.class,
                AsyncSearchPlugin.class,
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
                .put(AsyncSearchManagementService.REAPER_INTERVAL_SETTING.getKey(),  TimeValue.timeValueSeconds(5))
                .put(AsyncSearchManagementService.RESPONSE_CLEAN_UP_INTERVAL_SETTING.getKey(),  TimeValue.timeValueSeconds(5))
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

    public void testCleansUpExpiredAsyncSearchDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        SearchRequest searchRequest = client().prepareSearch("test").setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        //We need a NodeClient to make sure the listener gets injected in the search request execution.
        //Randomized client randomly return NodeClient/TransportClient
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsyncSearchRequest, plugins);
    }


    public void testCleansUpExpiredAsyncSearchDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();
        SearchRequest searchRequest = client().prepareSearch("test")
                .addScriptField("test_field",
                        new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())
                ).request();
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        testCase(internalCluster().smartClient(), submitAsyncSearchRequest, plugins);
    }

    public void testDeletesExpiredAsyncSearchResponseFromPersistedStore() throws Exception {
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

        final AtomicReference<AsyncSearchResponse> asyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<AsyncSearchResponse> nonExpiredAsyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        SearchRequest searchRequest = new SearchRequest(idx);
        SubmitAsyncSearchRequest submitAsyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest);
        submitAsyncSearchRequest.keepOnCompletion(true);
        submitAsyncSearchRequest.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        CountDownLatch latch = new CountDownLatch(2);
        client().execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                exceptionRef.set(asyncSearchResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        //submit another request to verify that the second request is not cancelled
        client().execute(SubmitAsyncSearchAction.INSTANCE, submitAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                nonExpiredAsyncSearchResponseRef.set(asyncSearchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        latch.await();
        waitUntil(() -> verifyResponsePersisted(asyncSearchResponseRef.get().getId()));
        waitUntil(() -> verifyResponsePersisted(nonExpiredAsyncSearchResponseRef.get().getId()));
        CountDownLatch updateLatch = new CountDownLatch(1);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(asyncSearchResponseRef.get().getId());
        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(1));
        client().execute(GetAsyncSearchAction.INSTANCE, getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                exceptionRef.set(asyncSearchResponse.getError());
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                updateLatch.countDown();
            }
        });
        updateLatch.await();
        waitUntil(() -> verifyResponseRemoved(asyncSearchResponseRef.get().getId()));
        assertBusy(() -> assertTrue(verifyResponsePersisted(nonExpiredAsyncSearchResponseRef.get().getId())));
        // delete the non expired response explicitly
        CountDownLatch deleteLatch = new CountDownLatch(1);
        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(nonExpiredAsyncSearchResponseRef.get().getId());
        client().execute(DeleteAsyncSearchAction.INSTANCE, deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
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

    private void testCase(Client client, SubmitAsyncSearchRequest request, List<ScriptedBlockPlugin> plugins) throws Exception {
        final AtomicReference<AsyncSearchResponse> asyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<AsyncSearchResponse> nonExpiredAsyncSearchResponseRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(2);
        client.execute(SubmitAsyncSearchAction.INSTANCE, request, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                exceptionRef.set(asyncSearchResponse.getError());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        //submit another request to verify that the second request is not cancelled
        client.execute(SubmitAsyncSearchAction.INSTANCE, request, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                nonExpiredAsyncSearchResponseRef.set(asyncSearchResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });
        latch.await();
        awaitForBlock(plugins);
        assertNotNull(asyncSearchResponseRef.get());
        CountDownLatch updateLatch = new CountDownLatch(1);
        GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(asyncSearchResponseRef.get().getId());
        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueMillis(1));
        client.execute(GetAsyncSearchAction.INSTANCE, getAsyncSearchRequest, new ActionListener<AsyncSearchResponse>() {
            @Override
            public void onResponse(AsyncSearchResponse asyncSearchResponse) {
                asyncSearchResponseRef.set(asyncSearchResponse);
                exceptionRef.set(asyncSearchResponse.getError());
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                updateLatch.countDown();
            }
        });
        updateLatch.await();
        assertThat(asyncSearchResponseRef.get().getExpirationTimeMillis(), lessThan(System.currentTimeMillis()));
        boolean cleanedUp = waitUntil(() -> verifyAsyncSearchDoesNotExists(asyncSearchResponseRef.get().getId()));
        assertTrue(cleanedUp);
        disableBlocks(plugins);
        AsyncSearchId asyncSearchId = AsyncSearchIdConverter.parseAsyncId(asyncSearchResponseRef.get().getId());
        TaskId taskId = new TaskId(asyncSearchId.getNode(), asyncSearchId.getTaskId());
        waitUntil(() -> verifyTaskCancelled(AsyncSearchTask.NAME, taskId));
        //ensure the second async search is not cleaned up
        assertBusy(() -> assertFalse(verifyAsyncSearchDoesNotExists(nonExpiredAsyncSearchResponseRef.get().getId())));
        logger.info("Segments {}", Strings.toString(client().admin().indices().prepareSegments("test").get()));
        CountDownLatch deleteLatch = new CountDownLatch(1);
        //explicitly clean up the second request
        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(nonExpiredAsyncSearchResponseRef.get().getId());
        client.execute(DeleteAsyncSearchAction.INSTANCE, deleteAsyncSearchRequest, new ActionListener<AcknowledgedResponse>() {
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
