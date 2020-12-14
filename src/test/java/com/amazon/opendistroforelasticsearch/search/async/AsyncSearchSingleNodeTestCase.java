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

package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public abstract class AsyncSearchSingleNodeTestCase extends ESSingleNodeTestCase {
    protected static final String INDEX = ".asynchronous_search_response";
    protected static final String TEST_INDEX = "index";

    @Override
    protected boolean resetNodeAfterTest() {
        return false;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createIndex(TEST_INDEX, Settings.builder().put("index.refresh_interval", -1).build());
        for (int i = 0; i < 10; i++)
            client().prepareIndex(TEST_INDEX, "type", String.valueOf(i)).setSource("field", "value" + i)
                    .setRefreshPolicy(IMMEDIATE).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        LinkedList<Class<? extends Plugin>> plugins = new LinkedList<>(super.getPlugins());
        plugins.add(SearchDelayPlugin.class);
        plugins.add(AsyncSearchPlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    public static ActionFuture<AsyncSearchResponse> executeSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        return client.execute(SubmitAsyncSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AsyncSearchResponse> executeGetAsyncSearch(Client client, GetAsyncSearchRequest request) {
        return client.execute(GetAsyncSearchAction.INSTANCE, request);
    }

    public static ActionFuture<AcknowledgedResponse> executeDeleteAsyncSearch(Client client, DeleteAsyncSearchRequest request) {
        return client.execute(DeleteAsyncSearchAction.INSTANCE, request);
    }

    public static void executeDeleteAsyncSearch(Client client, DeleteAsyncSearchRequest request,
                                                ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteAsyncSearchAction.INSTANCE, request, listener);
    }

    public static void executeSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request,
                                                ActionListener<AsyncSearchResponse> listener) {
        client.execute(SubmitAsyncSearchAction.INSTANCE, request, listener);
    }

    public static void executeGetAsyncSearch(Client client, GetAsyncSearchRequest request,
                                                ActionListener<AsyncSearchResponse> listener) {
        client.execute(GetAsyncSearchAction.INSTANCE, request, listener);
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

    public static class SearchDelayPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_delay";

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void disableBlock() {
            LogManager.getLogger(AsyncSearchSingleNodeTestCase.class).info("Disabling block ----------->");
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
                    LogManager.getLogger(AsyncSearchSingleNodeTestCase.class).info("Unblocked ----------->");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    @After
    public void tearDownData() throws InterruptedException {

        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
            deleteLatch.countDown();
        }));
        deleteLatch.await();
    }
}
