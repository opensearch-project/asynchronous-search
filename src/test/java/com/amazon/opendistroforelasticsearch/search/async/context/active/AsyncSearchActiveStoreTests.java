package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
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

import static com.amazon.opendistroforelasticsearch.search.async.AsyncSearchTestCase.mockAsyncSearchProgressListener;

public class AsyncSearchActiveStoreTests extends ESTestCase {
    private ClusterSettings clusterSettings;
    private ExecutorBuilder<?> executorBuilder;
    private int maxRunningContexts = 100;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder()
                .put("node.name", "test")
                .put("cluster.name", "ClusterServiceTests")
                .put("async_search.max_running_context", maxRunningContexts) //TODO setting not working fix this
                .build();
        final Set<Setting<?>> settingsSet =
                Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(
                        AsyncSearchActiveStore.MAX_RUNNING_CONTEXT)).collect(Collectors.toSet());
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30)));
        executorBuilder = executorBuilders.get(0);
        clusterSettings = new ClusterSettings(settings, settingsSet);
    }

    public void testPutContextRejection() throws InterruptedException, BrokenBarrierException, TimeoutException {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            AtomicInteger runningContexts = new AtomicInteger();
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsyncSearchActiveStore activeStore = new AsyncSearchActiveStore(mockClusterService);
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
                        AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(finalTestThreadPool1);
                        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                                runningContexts.incrementAndGet());
                        boolean keepOnCompletion = randomBoolean();
                        TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
                        AsyncSearchContextListener asyncSearchContextListener = new AsyncSearchContextListener() {
                        };
                        AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node.getId(),
                                keepAlive, keepOnCompletion, finalTestThreadPool1,
                                finalTestThreadPool1::absoluteTimeInMillis, asyncSearchProgressListener, asyncSearchContextListener, null);

                        activeStore.putContext(asyncSearchContextId, context, asyncSearchContextListener::onContextRejected);
                        numSuccesses.getAndIncrement();
                        Optional<AsyncSearchActiveContext> optional = activeStore.getContext(asyncSearchContextId);
                        assert (optional.isPresent());
                        assertEquals(optional.get(), context);
                        barrier.await(5, TimeUnit.SECONDS);
                        assertsLatch.await();
                        activeStore.freeContext(context.getContextId());
                        assertFalse(activeStore.getContext(context.getContextId()).isPresent());
                        barrier.await();
                    } catch (EsRejectedExecutionException e) {
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
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsyncSearchActiveStore activeStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            Optional<AsyncSearchActiveContext> optional = activeStore.getContext(asyncSearchContextId);
            assertFalse(optional.isPresent());
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testFreeNonExistentContext() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsyncSearchActiveStore activeStore = new AsyncSearchActiveStore(mockClusterService);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
            assertFalse(activeStore.freeContext(asyncSearchContextId));
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testContextFoundWithContextIdMismatch() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ThreadPool testThreadPool = null;
        try {
            testThreadPool = new TestThreadPool(this.getClass().getName(), executorBuilder);
            ClusterService mockClusterService = ClusterServiceUtils.createClusterService(testThreadPool, node, clusterSettings);
            AsyncSearchActiveStore activeStore = new AsyncSearchActiveStore(mockClusterService);
            long id = randomNonNegativeLong();
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(), id);
            assertFalse(activeStore.freeContext(new AsyncSearchContextId(UUID.randomUUID().toString(), id)));
            assertEquals(activeStore.getAllContexts().size(), 0);
        } finally {
            ThreadPool.terminate(testThreadPool, 10, TimeUnit.SECONDS);
        }
    }
}
