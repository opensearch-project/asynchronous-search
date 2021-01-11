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

package com.amazon.opendistroforelasticsearch.search.asynchronous.integTests;

import com.amazon.opendistroforelasticsearch.search.asynchronous.action.AsynchronousSearchStatsAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.AsynchronousSearchStatsRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchStatsResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.AsynchronousSearchCountStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.AsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.TestClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchIntegTestCase.ScriptedBlockPlugin.SCRIPT_NAME;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 5, scope = ESIntegTestCase.Scope.TEST)
public class AsynchronousSearchStatsIT extends AsynchronousSearchIntegTestCase {
    private int asConcurrentLimit = 20;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(AsynchronousSearchActiveStore.MAX_RUNNING_SEARCHES_SETTING.getKey(), asConcurrentLimit)
                .build();
    }

    public void testNodewiseStats() throws InterruptedException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest(index));
        submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(2));
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        List<DiscoveryNode> dataNodes = new LinkedList<>();
        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
            dataNodes.add(node.value);
        });
        assertFalse(dataNodes.isEmpty());
        DiscoveryNode randomDataNode = dataNodes.get(randomInt(dataNodes.size() - 1));
        try {
            AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(client(randomDataNode.getName()),
                    submitAsynchronousSearchRequest);
            assertNotNull(asResponse.getSearchResponse());
            TestClientUtils.assertResponsePersistence(client(), asResponse.getId());
            AsynchronousSearchStatsResponse statsResponse = client().execute(AsynchronousSearchStatsAction.INSTANCE,
                    new AsynchronousSearchStatsRequest()).get();
            statsResponse.getNodes().forEach(nodeStats -> {
                AsynchronousSearchCountStats asCountStats = nodeStats.getAsynchronousSearchCountStats();
                if (nodeStats.getNode().equals(randomDataNode)) {
                    assertEquals(1, asCountStats.getPersistedCount());
                    assertEquals(1, asCountStats.getCompletedCount());
                    assertEquals(1, asCountStats.getSubmittedCount());
                    assertEquals(1, asCountStats.getInitializedCount());
                    assertEquals(0, asCountStats.getFailedCount());
                    assertEquals(0, asCountStats.getRunningCount());
                    assertEquals(0, asCountStats.getCancelledCount());
                } else {
                    assertEquals(0, asCountStats.getPersistedCount());
                    assertEquals(0, asCountStats.getCompletedCount());
                    assertEquals(0, asCountStats.getFailedCount());
                    assertEquals(0, asCountStats.getRunningCount());
                }
            });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public void testStatsAcrossNodes() throws InterruptedException, ExecutionException {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(AsynchronousSearchStatsIT.class.getName());
            String index = "idx";
            createIndex(index);
            indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                            .setSource("field1", "the quick brown fox jumps"),
                    client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                    client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

            List<DiscoveryNode> dataNodes = new LinkedList<>();
            clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
                dataNodes.add(node.value);
            });
            assertFalse(dataNodes.isEmpty());
            int numThreads = 20;
            List<Runnable> threads = new ArrayList<>();
            AtomicLong expectedNumSuccesses = new AtomicLong();
            AtomicLong expectedNumFailures = new AtomicLong();
            AtomicLong expectedNumPersisted = new AtomicLong();
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                threads.add(() -> {
                    try {
                        boolean success = randomBoolean();
                        boolean keepOnCompletion = randomBoolean();
                        if (keepOnCompletion) {
                            expectedNumPersisted.getAndIncrement();
                        }
                        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest;
                        if (success) {
                            expectedNumSuccesses.getAndIncrement();
                            submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest(index));
                            submitAsynchronousSearchRequest.waitForCompletionTimeout(TimeValue.timeValueSeconds(2));
                            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);

                        } else {
                            expectedNumFailures.getAndIncrement();
                            submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(new SearchRequest(
                                    "non_existent_index"));
                            submitAsynchronousSearchRequest.keepOnCompletion(keepOnCompletion);
                        }

                        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(client(
                                dataNodes.get(randomInt(1)).getName()),
                                submitAsynchronousSearchRequest);
                        if (keepOnCompletion) {
                            TestClientUtils.assertResponsePersistence(client(), asResponse.getId());
                        }
                    } catch (Exception e) {
                        fail(e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }
            TestThreadPool finalThreadPool = threadPool;
            threads.forEach(t -> finalThreadPool.generic().execute(t));
            latch.await();
            AsynchronousSearchStatsResponse statsResponse = client().execute(AsynchronousSearchStatsAction.INSTANCE,
                    new AsynchronousSearchStatsRequest()).get();
            AtomicLong actualNumSuccesses = new AtomicLong();
            AtomicLong actualNumFailures = new AtomicLong();
            AtomicLong actualNumPersisted = new AtomicLong();
            for (AsynchronousSearchStats node : statsResponse.getNodes()) {
                AsynchronousSearchCountStats asCountStats = node.getAsynchronousSearchCountStats();
                assertEquals(asCountStats.getRunningCount(), 0);

                assertThat(expectedNumSuccesses.get(), greaterThanOrEqualTo(asCountStats.getCompletedCount()));
                actualNumSuccesses.getAndAdd(asCountStats.getCompletedCount());

                assertThat(expectedNumFailures.get(), greaterThanOrEqualTo(asCountStats.getFailedCount()));
                actualNumFailures.getAndAdd(asCountStats.getFailedCount());

                assertThat(expectedNumPersisted.get(), greaterThanOrEqualTo(asCountStats.getPersistedCount()));
                actualNumPersisted.getAndAdd(asCountStats.getPersistedCount());
            }

            assertEquals(expectedNumPersisted.get(), actualNumPersisted.get());
            assertEquals(expectedNumFailures.get(), actualNumFailures.get());
            assertEquals(expectedNumSuccesses.get(), actualNumSuccesses.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testRunningAsynchronousSearchCountStat() throws InterruptedException, ExecutionException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
        submitAsynchronousSearchRequest.keepOnCompletion(true);
        AsynchronousSearchResponse asResponse = executeSubmitAsynchronousSearch(client(), submitAsynchronousSearchRequest);
        AsynchronousSearchStatsResponse statsResponse = client().execute(AsynchronousSearchStatsAction.INSTANCE,
                new AsynchronousSearchStatsRequest()).get();
        long runningSearchCount = 0;
        for (AsynchronousSearchStats node : statsResponse.getNodes()) {
            runningSearchCount += node.getAsynchronousSearchCountStats().getRunningCount();
            assertEquals(node.getAsynchronousSearchCountStats().getCompletedCount(), 0L);
            assertEquals(node.getAsynchronousSearchCountStats().getFailedCount(), 0L);
            assertEquals(node.getAsynchronousSearchCountStats().getPersistedCount(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
        disableBlocks(plugins);
        TestClientUtils.assertResponsePersistence(client(), asResponse.getId());
        statsResponse = client().execute(AsynchronousSearchStatsAction.INSTANCE, new AsynchronousSearchStatsRequest()).get();
        long persistedCount = 0;
        long completedCount = 0;
        for (AsynchronousSearchStats node : statsResponse.getNodes()) {
            persistedCount += node.getAsynchronousSearchCountStats().getPersistedCount();
            completedCount += node.getAsynchronousSearchCountStats().getCompletedCount();
            assertEquals(node.getAsynchronousSearchCountStats().getRunningCount(), 0L);
            assertEquals(node.getAsynchronousSearchCountStats().getFailedCount(), 0L);
        }
        assertEquals(runningSearchCount, 1L);
    }

    public void testThrottledAsynchronousSearchCount() throws InterruptedException, ExecutionException {
        String index = "idx";
        createIndex(index);
        indexRandom(super.ignoreExternalCluster(), client().prepareIndex(index, "type1", "1")
                        .setSource("field1", "the quick brown fox jumps"),
                client().prepareIndex(index, "type1", "2").setSource("field1", "quick brown"),
                client().prepareIndex(index, "type1", "3").setSource("field1", "quick"));

        List<DiscoveryNode> dataNodes = new LinkedList<>();
        clusterService().state().nodes().getDataNodes().iterator().forEachRemaining(node -> {
            dataNodes.add(node.value);
        });
        assertFalse(dataNodes.isEmpty());
        DiscoveryNode randomDataNode = dataNodes.get(randomInt(dataNodes.size() - 1));
        int numThreads = 21;
        List<Thread> threads = new ArrayList<>();
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        SearchRequest searchRequest = client().prepareSearch(index).setQuery(
                scriptQuery(new Script(
                        ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .request();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                try {
                    SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest = new SubmitAsynchronousSearchRequest(searchRequest);
                    executeSubmitAsynchronousSearch(client(randomDataNode.getName()), submitAsynchronousSearchRequest);
                } catch (ExecutionException e) {
                    assertThat(e.getMessage(), containsString("Trying to create too many running contexts"));
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(verifyThrottlingFromStats());
        disableBlocks(plugins);
    }

    private boolean verifyThrottlingFromStats() {
        try {
            AsynchronousSearchStatsResponse statsResponse = client().execute(AsynchronousSearchStatsAction.INSTANCE,
                    new AsynchronousSearchStatsRequest()).get();
            for (AsynchronousSearchStats nodeStats : statsResponse.getNodes()) {
                if (nodeStats.getAsynchronousSearchCountStats().getThrottledCount() == 1L) {
                    return true;
                }
            }
            return false;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }
}
