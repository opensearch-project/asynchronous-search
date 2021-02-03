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


package com.amazon.opendistroforelasticsearch.search.asynchronous.context.active;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.commons.AsynchronousSearchTestCase;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.task.AsynchronousSearchTask;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.TestClientUtils;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;

public class AsynchronousSearchActiveContextTests extends AsynchronousSearchTestCase {

    public void testInitializeContext() {
        TestThreadPool threadPool = null;
        try {
            int writeThreadPoolSize = randomIntBetween(1, 2);
            int writeThreadPoolQueueSize = randomIntBetween(1, 2);
            Settings settings = Settings.builder()
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(threadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            User user = TestClientUtils.randomUser();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node,
                    keepAlive, keepOnCompletion, threadPool, threadPool::absoluteTimeInMillis, asProgressListener, user, () -> true);
            assertEquals(AsynchronousSearchState.INIT, context.getAsynchronousSearchState());
            assertNull(context.getTask());
            assertNull(context.getAsynchronousSearchId());
            assertEquals(context.getAsynchronousSearchState(), AsynchronousSearchState.INIT);
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
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(threadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            User user = TestClientUtils.randomUser();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asProgressListener, user, () -> true);
            SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(new SearchRequest("test"));
            request.keepAlive(keepAlive);
            request.keepOnCompletion(keepOnCompletion);
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport",
                    SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap(), context, request, (c) -> {});
            context.setTask(task);
            assertEquals(task, context.getTask());
            assertEquals(task.getStartTime(), context.getStartTimeMillis());
            assertEquals(task.getStartTime() + keepAlive.getMillis(), context.getExpirationTimeMillis());
            String description = task.getDescription();
            assertThat(description, containsString("[asynchronous search]"));
            assertThat(description, containsString("indices[test]"));
            assertThat(description, containsString("keep_alive[" + keepAlive + "]"));
            assertThat(description, containsString("keep_on_completion[" + keepOnCompletion + "]"));
            assertTrue(context.isAlive());
            assertFalse(context.isExpired());
            expectThrows(SetOnce.AlreadySetException.class, () -> context.setTask(task));
            if (keepOnCompletion) {
                assertTrue(context.shouldPersist());
            } else {
                assertFalse(context.shouldPersist());
            }
            assertTrue(user.equals(context.getUser()));
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
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(threadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = true;
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
            threadPool::absoluteTimeInMillis, asProgressListener, null, () -> true);
            if (randomBoolean()) {
                SearchResponse mockSearchResponse = getMockSearchResponse();
                try {
                    context.processSearchResponse(mockSearchResponse);
                } catch (Exception ex) {
                    fail("Unexpected exception "+ ex);
                }
                if (mockSearchResponse.equals(context.getSearchResponse())) {
                    assertNull(context.getSearchError());
                }
            } else {
                RuntimeException e = new RuntimeException(UUID.randomUUID().toString());
                try {
                    context.processSearchFailure(e);
                } catch (Exception ex) {
                    fail("Unexpected exception "+ ex);
                }
                if (e.equals(context.getSearchError())) {
                    assertNull(context.getSearchResponse());
                }
            }

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
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".size", writeThreadPoolSize)
                    .put("thread_pool." + AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                            + ".queue_size", writeThreadPoolQueueSize)
                    .build();
            final int availableProcessors = EsExecutors.allocatedProcessors(settings);
            ScalingExecutorBuilder scalingExecutorBuilder =
                    new ScalingExecutorBuilder(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                            Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
            threadPool = new TestThreadPool("Tests", settings, scalingExecutorBuilder);
            String node = UUID.randomUUID().toString();
            AsynchronousSearchProgressListener asProgressListener = mockAsynchronousSearchProgressListener(threadPool);
            AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(),
                    randomNonNegativeLong());
            boolean keepOnCompletion = randomBoolean();
            TimeValue keepAlive = TimeValue.timeValueDays(randomInt(100));
            AsynchronousSearchActiveContext context = new AsynchronousSearchActiveContext(asContextId, node,
                    keepAlive, keepOnCompletion, threadPool,
                    threadPool::absoluteTimeInMillis, asProgressListener, null, () -> true);
            AsynchronousSearchTask task = new AsynchronousSearchTask(randomNonNegativeLong(), "transport",
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
        } finally {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        }
    }

    protected SearchResponse getMockSearchResponse() {
        return new SearchResponse(new InternalSearchResponse(
                new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                InternalAggregations.from(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                new SearchProfileShardResults(Collections.emptyMap()), false, false, 1),
                "", 1, 1, 0, 0,
                ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}

