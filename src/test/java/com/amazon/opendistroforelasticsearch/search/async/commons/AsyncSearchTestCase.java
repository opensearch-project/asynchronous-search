package com.amazon.opendistroforelasticsearch.search.async.commons;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Function;

public abstract class AsyncSearchTestCase extends ESTestCase {

    public static AsyncSearchProgressListener  mockAsyncSearchProgressListener(ThreadPool threadPool) {
        return new AsyncSearchProgressListener(threadPool.absoluteTimeInMillis(), r -> null, e -> null, threadPool.generic(),
                threadPool::relativeTimeInMillis, () -> getReduceContextBuilder());
    }

    public static AsyncSearchProgressListener mockAsyncSearchProgressListener(ThreadPool threadPool,
                                                                       Function<SearchResponse, AsyncSearchResponse> successFunction,
                                                                       Function<Exception, AsyncSearchResponse> failureFunction) {
        return new AsyncSearchProgressListener(threadPool.absoluteTimeInMillis(), successFunction, failureFunction, threadPool.generic(),
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
