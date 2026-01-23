/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.listener;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.asynchronous.response.AsynchronousSearchProgress;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AsynchronousSearchProgressListenerTests extends OpenSearchTestCase {

    public void testProgressTracksShardMaxDocStats() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(getClass().getName());
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = new InternalAggregation.ReduceContextBuilder() {
                @Override
                public InternalAggregation.ReduceContext forPartialReduction() {
                    return InternalAggregation.ReduceContext.forPartialReduction(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        null,
                        () -> PipelineAggregator.PipelineTree.EMPTY
                    );
                }

                @Override
                public InternalAggregation.ReduceContext forFinalReduction() {
                    return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        null,
                        b -> {},
                        PipelineAggregator.PipelineTree.EMPTY
                    );
                }
            };
            AsynchronousSearchProgressListener listener = new AsynchronousSearchProgressListener(
                threadPool.relativeTimeInMillis(),
                response -> null,
                exception -> null,
                threadPool.generic(),
                threadPool::relativeTimeInMillis,
                () -> reduceContextBuilder
            );
            List<SearchShard> shards = Collections.singletonList(new SearchShard(null, new ShardId("index", "uuid", 0)));
            listener.onListShards(shards, Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);
            listener.onQueryResult(0, 7L, 20L);

            AsynchronousSearchProgress progress = listener.progress();
            assertNotNull(progress);
            assertEquals(1, progress.getShards().size());
            AsynchronousSearchProgress.ShardProgress shardProgress = progress.getShards().get(0);
            assertEquals("index", shardProgress.getIndex());
            assertEquals(0, shardProgress.getShardId());
            assertEquals(7L, shardProgress.getMaxDocIdProcessed());
            assertEquals(20L, shardProgress.getMaxDoc());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }

    public void testProgressMarksShardCompleteOnQueryResult() {
        TestThreadPool threadPool = null;
        try {
            threadPool = new TestThreadPool(getClass().getName());
            InternalAggregation.ReduceContextBuilder reduceContextBuilder = new InternalAggregation.ReduceContextBuilder() {
                @Override
                public InternalAggregation.ReduceContext forPartialReduction() {
                    return InternalAggregation.ReduceContext.forPartialReduction(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        null,
                        () -> PipelineAggregator.PipelineTree.EMPTY
                    );
                }

                @Override
                public InternalAggregation.ReduceContext forFinalReduction() {
                    return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        null,
                        b -> {},
                        PipelineAggregator.PipelineTree.EMPTY
                    );
                }
            };
            AsynchronousSearchProgressListener listener = new AsynchronousSearchProgressListener(
                threadPool.relativeTimeInMillis(),
                response -> null,
                exception -> null,
                threadPool.generic(),
                threadPool::relativeTimeInMillis,
                () -> reduceContextBuilder
            );
            List<SearchShard> shards = Collections.singletonList(new SearchShard(null, new ShardId("index", "uuid", 0)));
            listener.onListShards(shards, Collections.emptyList(), SearchResponse.Clusters.EMPTY, false);
            listener.onQueryResult(0, 3L, 10L);

            AsynchronousSearchProgress progress = listener.progress();
            assertNotNull(progress);
            assertEquals(3L, progress.getShards().get(0).getMaxDocIdProcessed());

            listener.onQueryResult(0);
            AsynchronousSearchProgress completedProgress = listener.progress();
            assertNotNull(completedProgress);
            assertEquals(10L, completedProgress.getShards().get(0).getMaxDocIdProcessed());
        } finally {
            ThreadPool.terminate(threadPool, 100, TimeUnit.MILLISECONDS);
        }
    }
}
