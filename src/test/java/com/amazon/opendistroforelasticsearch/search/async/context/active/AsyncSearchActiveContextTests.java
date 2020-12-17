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


package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.CLOSED;
import static java.util.Collections.emptyMap;

public class AsyncSearchActiveContextTests extends AsyncSearchTestCase {

    public void testInitializeContext() {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(threadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            assertEquals(AsyncSearchState.INIT, context.getAsyncSearchState());
            assertNull(context.getTask());
            assertNull(context.getAsyncSearchId());
            assertEquals(context.getAsyncSearchState(), AsyncSearchState.INIT);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testTaskBootstrap() {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(threadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null, (c) -> {
            });
            context.setTask(task);
            assertEquals(task, context.getTask());
            assertEquals(task.getStartTime(), context.getStartTimeMillis());
            assertEquals(task.getStartTime() + keepAlive.getMillis(), context.getExpirationTimeMillis());
            assertTrue(context.isAlive());
            assertFalse(context.isExpired());
            expectThrows(SetOnce.AlreadySetException.class, () -> context.setTask(task));
            if (keepOnCompletion) {
                assertTrue(context.shouldPersist());
            } else {
                assertFalse(context.shouldPersist());
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    public void testProcessSearchCompletion() throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(threadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = true;
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });

            int numThreads = 10;
            AtomicInteger numSuccesses = new AtomicInteger();
            List<Runnable> runnables = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(numThreads);
            for (int i = 0; i < numThreads; i++) {
                Runnable runnable = () -> {
                    if (randomBoolean()) {
                        SearchResponse mockSearchResponse = getMockSearchResponse();
                        context.processSearchResponse(mockSearchResponse);
                        if (mockSearchResponse.equals(context.getSearchResponse())) {
                            numSuccesses.getAndIncrement();
                            assertNull(context.getSearchError());
                        }
                    } else {
                        RuntimeException e = new RuntimeException(UUID.randomUUID().toString());
                        context.processSearchFailure(e);
                        if (e.equals(context.getSearchError())) {
                            numSuccesses.getAndIncrement();
                            assertNull(context.getSearchResponse());
                        }
                    }
                    countDownLatch.countDown();
                };
                runnables.add(runnable);
            }
            for (Runnable r : runnables) {
                threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME).execute(r);
            }
            countDownLatch.await();
            assertEquals(numSuccesses.get(), 1);
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }


    public void testClosedContext() throws InterruptedException {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsyncSearchProgressListener asyncSearchProgressListener = mockAsyncSearchProgressListener(threadPool);
            AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsyncSearchActiveContext context = new AsyncSearchActiveContext(asyncSearchContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asyncSearchProgressListener, new AsyncSearchContextListener() {
            });
            AsyncSearchTask task = new AsyncSearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, null, (c) -> {
            });
            context.setTask(task);
            assertEquals(task, context.getTask());
            assertEquals(task.getStartTime(), context.getStartTimeMillis());
            assertEquals(task.getStartTime() + keepAlive.getMillis(), context.getExpirationTimeMillis());
            assertTrue(context.isAlive());
            assertFalse(context.isExpired());
            expectThrows(SetOnce.AlreadySetException.class, () -> context.setTask(task));
            context.setState(CLOSED);
            context.close();
            assertFalse(context.isAlive());
            expectThrows(AssertionError.class, () -> context.setExpirationTimeMillis(randomNonNegativeLong()));
            expectThrows(AssertionError.class, () -> context.setTask(task));
            if (keepOnCompletion) {
                assertTrue(keepOnCompletion);
            } else {
                assertFalse(keepOnCompletion);
            }
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                new InternalAggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

