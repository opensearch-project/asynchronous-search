/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.commons;

import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.painless.PainlessModulePlugin;
import org.opensearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.GetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.search.asynchronous.service.AsynchronousSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.TotalHits;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.painless.PainlessModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public abstract class AsynchronousSearchSingleNodeTestCase extends OpenSearchSingleNodeTestCase {
    protected static final String INDEX = AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX;
    protected static final String TEST_INDEX = "index";

    @Override
    protected boolean resetNodeAfterTest() {
        return false;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<Long, AsynchronousSearchActiveContext> allActiveContexts = getInstanceFromNode(AsynchronousSearchService.class)
            .getAllActiveContexts();
        assertTrue(allActiveContexts.toString(), allActiveContexts.isEmpty());
        createIndex(TEST_INDEX, Settings.builder().put("index.refresh_interval", -1).build());
        for (int i = 0; i < 10; i++)
            client().prepareIndex(TEST_INDEX).setId(String.valueOf(i)).setSource("field", "value" + i).setRefreshPolicy(IMMEDIATE).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        LinkedList<Class<? extends Plugin>> plugins = new LinkedList<>(super.getPlugins());
        plugins.add(SearchDelayPlugin.class);
        plugins.add(AsynchronousSearchPlugin.class);
        plugins.add(ReindexModulePlugin.class);
        plugins.add(PainlessModulePlugin.class);
        return plugins;
    }

    public static ActionFuture<AsynchronousSearchResponse> executeSubmitAsynchronousSearch(
        Client client,
        SubmitAsynchronousSearchRequest request
    ) {
        return client.execute(SubmitAsynchronousSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AsynchronousSearchResponse> executeGetAsynchronousSearch(
        Client client,
        GetAsynchronousSearchRequest request
    ) {
        return client.execute(GetAsynchronousSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AcknowledgedResponse> executeDeleteAsynchronousSearch(
        Client client,
        DeleteAsynchronousSearchRequest request
    ) {
        return client.execute(DeleteAsynchronousSearchAction.INSTANCE, request);
    }

    public static void executeDeleteAsynchronousSearch(
        Client client,
        DeleteAsynchronousSearchRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        client.execute(DeleteAsynchronousSearchAction.INSTANCE, request, listener);
    }

    public static void executeSubmitAsynchronousSearch(
        Client client,
        SubmitAsynchronousSearchRequest request,
        ActionListener<AsynchronousSearchResponse> listener
    ) {
        client.execute(SubmitAsynchronousSearchAction.INSTANCE, request, listener);
    }

    public static void executeGetAsynchronousSearch(
        Client client,
        GetAsynchronousSearchRequest request,
        ActionListener<AsynchronousSearchResponse> listener
    ) {
        client.execute(GetAsynchronousSearchAction.INSTANCE, request, listener);
    }

    public static boolean verifyAsynchronousSearchState(Client client, String id, AsynchronousSearchState state) {
        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(id);
        try {
            AsynchronousSearchResponse asResponse = executeGetAsynchronousSearch(client, getAsynchronousSearchRequest).actionGet();
            return asResponse.getState() == state;
        } catch (Exception ex) {
            fail("Exception received on trying to retrieve state " + ex.getMessage());
        }
        return false;
    }

    public List<SearchDelayPlugin> initPluginFactory() {
        List<SearchDelayPlugin> plugins = new ArrayList<>();
        PluginsService pluginsService = getInstanceFromNode(PluginsService.class);
        plugins.addAll(pluginsService.filterPlugins(SearchDelayPlugin.class));
        enableBlocks(plugins);
        return plugins;
    }

    public void disableBlocks(List<SearchDelayPlugin> plugins) {
        for (SearchDelayPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public void enableBlocks(List<SearchDelayPlugin> plugins) {
        for (SearchDelayPlugin plugin : plugins) {
            plugin.enableBlock();
        }
    }

    protected SearchResponse getMockSearchResponse() {
        int totalShards = randomInt(100);
        int successfulShards = totalShards - randomInt(100);
        return new SearchResponse(
            new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()),
                false,
                false,
                randomInt(5)
            ),
            "",
            totalShards,
            successfulShards,
            0,
            randomNonNegativeLong(),
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    public static class SearchDelayPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_delay";

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void disableBlock() {
            LogManager.getLogger(AsynchronousSearchSingleNodeTestCase.class).info("Disabling block ----------->");
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    // multi-threaded tests taking longer than the default 10s
                    assertBusy(() -> assertFalse(shouldBlock.get()), 60, TimeUnit.SECONDS);
                    LogManager.getLogger(AsynchronousSearchSingleNodeTestCase.class).info("Unblocked ----------->");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    @After
    public void tearDownData() throws InterruptedException {
        logger.warn("deleting asynchronous search response index");
        waitUntil(() -> getInstanceFromNode(AsynchronousSearchService.class).getAllActiveContexts().isEmpty());
        logger.warn("delete asynchronous search response index");
        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin()
            .indices()
            .prepareDelete(INDEX)
            .execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> { deleteLatch.countDown(); }));
        deleteLatch.await();
    }

    protected void assertDocNotPresentInAsynchronousSearchResponseIndex(String id) {
        try {
            assertFalse(client().get(new GetRequest(INDEX).refresh(true).id(id)).actionGet().isExists());
        } catch (Exception e) {
            assertTrue(e instanceof IndexNotFoundException);
        }
    }

    protected void assertAsynchronousSearchResourceCleanUp(String id) {
        assertDocNotPresentInAsynchronousSearchResponseIndex(id);
        AsynchronousSearchService asService = getInstanceFromNode(AsynchronousSearchService.class);
        Map<Long, AsynchronousSearchActiveContext> activeContexts = asService.getAllActiveContexts();
        assertTrue(activeContexts.isEmpty());
    }
}
