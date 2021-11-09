/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.commons;

import org.opensearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Function;

public abstract class AsynchronousSearchTestCase extends OpenSearchTestCase {

    public static AsynchronousSearchProgressListener mockAsynchronousSearchProgressListener(ThreadPool threadPool) {
        return new AsynchronousSearchProgressListener(threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                threadPool::relativeTimeInMillis, () -> getReduceContextBuilder());
    }

    public static AsynchronousSearchProgressListener mockAsynchronousSearchProgressListener(ThreadPool threadPool,
                                                                       Function<SearchResponse, AsynchronousSearchResponse> successFunction,
                                                                       Function<Exception, AsynchronousSearchResponse> failureFunction) {
        return new AsynchronousSearchProgressListener(threadPool.absoluteTimeInMillis(), successFunction, failureFunction,
                threadPool.generic(),
                threadPool::relativeTimeInMillis, () -> getReduceContextBuilder());
    }

    private static InternalAggregation.ReduceContextBuilder getReduceContextBuilder() {
        return new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(BigArrays.NON_RECYCLING_INSTANCE, null,
                        null);
            }

            public InternalAggregation.ReduceContext forFinalReduction() {
                return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineAggregator.PipelineTree.EMPTY);
            }};
    }
}
