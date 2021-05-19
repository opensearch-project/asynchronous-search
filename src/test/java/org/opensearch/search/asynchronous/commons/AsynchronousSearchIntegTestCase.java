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

package org.opensearch.search.asynchronous.commons;

import org.opensearch.search.asynchronous.action.GetAsynchronousSearchAction;
import org.opensearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.task.AsynchronousSearchTask;
import org.apache.logging.log4j.LogManager;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.index.reindex.ReindexPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.search.lookup.LeafFieldsLookup;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.greaterThan;

public abstract class AsynchronousSearchIntegTestCase extends OpenSearchIntegTestCase {

    protected static final String TEST_INDEX = "index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ScriptedBlockPlugin.class,
                AsynchronousSearchPlugin.class,
                ReindexPlugin.class);
    }

    @Override
    protected double getPerTestTransportClientRatio() {
        return 0;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return Math.min(2, cluster().numDataNodes() - 1);
    }

    protected List<ScriptedBlockPlugin> initBlockFactory() {
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

    protected void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
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

    protected void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    protected SearchResponse ensureSearchWasCancelled(SearchResponse searchResponse, Exception e) {
        try {
            if (searchResponse != null) {
                logger.info("Search response {}", searchResponse);
                assertNotEquals("At least one shard should have failed", 0, searchResponse.getFailedShards());
                return searchResponse;
            } else {
                throw e;
            }
        } catch (SearchPhaseExecutionException ex) {
            logger.info("All shards failed with", ex);
            return null;
        } catch (Exception exception) {
            fail("Unexpected exception " + e.getMessage());
            return null;
        }
    }

    protected boolean verifyAsynchronousSearchDoesNotExists(String id) {
        GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(id);
        try {
            AsynchronousSearchResponse response = client().execute(GetAsynchronousSearchAction.INSTANCE,
                    getAsynchronousSearchRequest).actionGet();
            return response == null;
        } catch (Exception e) {
            if (e instanceof ResourceNotFoundException) {
                return true;
            } else {
                fail("failed to executed get for id" + e.getMessage());
            }
        }
        return true;
    }

    protected boolean verifyResponsePersisted(String id) {
        try {
            boolean isExists = client().get(new GetRequest(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                    .refresh(true).id(id))
                    .actionGet().isExists();
            return isExists;
        } catch (ResourceNotFoundException | NoShardAvailableActionException e) {
            return false;
        } catch (Exception ex) {
            fail("Failed to verify persistence " + ex.getMessage());
        }
        return false;
    }

    protected boolean verifyResponseRemoved(String id) {
        return verifyResponsePersisted(id) == false;
    }

    protected boolean verifyTaskCancelled(String action, TaskId taskId) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).setTaskId(taskId).get();
        return listTasksResponse.getTasks().size() == 0;
    }

    protected boolean verifyTaskCancelled(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        return listTasksResponse.getTasks().size() == 0;
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_block";

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
                LogManager.getLogger(AsynchronousSearchIntegTestCase.class).info("Blocking on the document {}",
                        fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()), 60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    public static AsynchronousSearchResponse executeSubmitAsynchronousSearch(Client client, SubmitAsynchronousSearchRequest request)
            throws ExecutionException, InterruptedException {
        return client.execute(SubmitAsynchronousSearchAction.INSTANCE, request).get();
    }

    //We need to apply blocks via ScriptedBlockPlugin, external clusters are immutable
    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    protected void waitForAsyncSearchTasksToComplete() throws InterruptedException {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions(AsynchronousSearchTask.NAME);
        waitUntil(() -> client().execute(ListTasksAction.INSTANCE, listTasksRequest).actionGet().getTasks().isEmpty());
    }
}
