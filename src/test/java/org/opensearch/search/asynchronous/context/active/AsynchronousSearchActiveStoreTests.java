/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.active;

import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.search.asynchronous.commons.AsynchronousSearchTestCase.mockAsynchronousSearchProgressListener;

public class AsynchronousSearchActiveStoreTests extends OpenSearchTestCase {
    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private int maxRunningContexts = 20;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put(AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey(), maxRunningContexts)
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsynchronousSearchActiveStore.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING)).collect(Collectors.toSet());
        final int availableProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testPutContextRejection() throws InterruptedException, BrokenBarrierException, TimeoutException {
        DiscoveryNode node = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            AtomicInteger runningContexts = new AtomicInteger();
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsynchronousSearchActiveStore activeStore = new AsynchronousSearchActiveStore(mockClusterService);
            List<Runnable> runnables = new ArrayList<>();
            AtomicInteger numRejected = new AtomicInteger();
            AtomicInteger numFailures = new AtomicInteger();
            AtomicInteger numSuccesses = new AtomicInteger();
            int numThreads = maxRunningContexts + 1;
            CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
            CountDownLatch assertsLatch = new CountDownLatch(1);
            for (int i = 0; i < numThreads; i++) {
                ThreadPool finalTestThreadPool1 = testThreadPool;
                runnables.add(() -> {
                    try {
                        AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(
                                finalTestThreadPool1);
                        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                                runningContexts.incrementAndGet());
                        boolean keepOnCompletion = randomBoolean();
                        TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
                        AsynchronousSearchContextEventListener asContextEventListener = new AsynchronousSearchContextEventListener() {
                        };
                        AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node.getId(),
                                keepAlive, keepOnCompletion, finalTestThreadPool1,
                                finalTestThreadPool1::absoluteTimeInMillis, asProgressListener, null, () -> true);
                        activeStore.putContext(asContextId, context, asContextEventListener::onContextRejected);
                        numSuccesses.getAndIncrement();
                        Optional<AsynchronousSearchActiveContext> optional = activeStore.getContext(asContextId);
                        assert (optional.isPresent());
                        assertEquals(optional.get(), context);
                        barrier.await(5, TimeUnit.SECONDS);
                        assertsLatch.await();
                        activeStore.freeContext(context.getContextId());
                        assertFalse(activeStore.getContext(context.getContextId()).isPresent());
                        barrier.await();
                    } catch (OpenSearchRejectedExecutionException e) {
                        numRejected.getAndIncrement();
                        try {
                            barrier.await();
                            barrier.await();
                        } catch (InterruptedException | BrokenBarrierException ex) {
                            numFailures.getAndIncrement();
                        }
                    } catch (InterruptedException | BrokenBarrierException e) {
                        numFailures.getAndIncrement();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        numFailures.getAndIncrement();
                    }
                });
            }
            ThreadPool finalTestThreadPool = testThreadPool;
            runnables.forEach(r -> finalTestThreadPool.generic().execute(r));
            barrier.await(5, TimeUnit.SECONDS);//create contexts
            assertEquals(activeStore.getAllContexts().size(), maxRunningContexts);
            assertEquals(numFailures.get(), 0);
            assertEquals(numRejected.get(), 1);
            assertEquals(numSuccesses.get(), maxRunningContexts);
            assertsLatch.countDown();
            barrier.await(5, TimeUnit.SECONDS); //free contexts
            assertEquals(activeStore.getAllContexts().size(), 0);
            assertEquals(numFailures.get(), 0);

        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }

    }

    public void testGetNonExistentContext() {
        DiscoveryNode node = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsynchronousSearchActiveStore activeStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            Optional<AsynchronousSearchActiveContext> optional = activeStore.getContext(asContextId);
            assertFalse(optional.isPresent());
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testFreeNonExistentContext() {
        DiscoveryNode node = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsynchronousSearchActiveStore activeStore = new AsynchronousSearchActiveStore(mockClusterService);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            assertFalse(activeStore.freeContext(asContextId));
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testContextFoundWithContextIdMismatch() {
        DiscoveryNode node = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsynchronousSearchActiveStore activeStore = new AsynchronousSearchActiveStore(mockClusterService);
            long id = randomNonNegativeLong();
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(), id);
            assertFalse(activeStore.freeContext(new AsynchronousSearchContextId(UUID.randomUUID().toString(), id)));
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }
}
