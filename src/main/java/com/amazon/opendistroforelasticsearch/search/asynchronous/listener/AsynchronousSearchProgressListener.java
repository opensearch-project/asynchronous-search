/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.asynchronous.listener;

import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;


/***
 * The implementation of {@link CompositeSearchProgressActionListener} responsible for updating the partial results of a single asynchronous
 * search request. All partial results are updated atomically.
 */
public class AsynchronousSearchProgressListener extends SearchProgressActionListener {

    private PartialResultsHolder partialResultsHolder;
    private final CompositeSearchProgressActionListener<AsynchronousSearchResponse> searchProgressActionListener;
    private final Function<SearchResponse, AsynchronousSearchResponse> successFunction;
    private final Function<Exception, AsynchronousSearchResponse> failureFunction;
    private final ExecutorService executor;

    public AsynchronousSearchProgressListener(long relativeStartMillis, Function<SearchResponse,
            AsynchronousSearchResponse> successFunction,
                                       Function<Exception, AsynchronousSearchResponse> failureFunction,
                                       ExecutorService executor, LongSupplier relativeTimeSupplier,
                                       Supplier<InternalAggregation.ReduceContextBuilder> reduceContextBuilder) {
        this.successFunction = successFunction;
        this.failureFunction = failureFunction;
        this.executor = executor;
        this.partialResultsHolder = new PartialResultsHolder(relativeStartMillis, relativeTimeSupplier, reduceContextBuilder);
        this.searchProgressActionListener = new CompositeSearchProgressActionListener<AsynchronousSearchResponse>();
    }


    /***
     * Returns the partial response for the search response.
     * @return the partial search response
     */
    public SearchResponse partialResponse() {
        return partialResultsHolder.partialResponse();
    }

    @Override
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, SearchResponse.Clusters clusters,
                                boolean fetchPhase) {
        partialResultsHolder.hasFetchPhase.set(fetchPhase);
        partialResultsHolder.totalShards.set(shards.size());
        partialResultsHolder.skippedShards.set(skippedShards.size());
        partialResultsHolder.successfulShards.set(skippedShards.size());
        partialResultsHolder.clusters.set(clusters);
        partialResultsHolder.isInitialized = true;
    }

    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        assert reducePhase > partialResultsHolder.reducePhase.get() : "reduce phase " + reducePhase + "less than previous phase"
                + partialResultsHolder.reducePhase.get();
        partialResultsHolder.partialInternalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        assert reducePhase > partialResultsHolder.reducePhase.get() : "reduce phase " + reducePhase + "less than previous phase"
                + partialResultsHolder.reducePhase.get();
        partialResultsHolder.internalAggregations.set(aggs);
        //we don't need to hold its reference beyond this point
        partialResultsHolder.partialInternalAggregations.set(null);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onShardResult(shardIndex);
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onQueryResult(int shardIndex) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onShardResult(shardIndex);
    }

    private synchronized void onShardResult(int shardIndex) {
        if (partialResultsHolder.successfulShardIds.contains(shardIndex) == false) {
            partialResultsHolder.successfulShardIds.add(shardIndex);
            partialResultsHolder.successfulShards.incrementAndGet();
        }
    }

    private synchronized void onSearchFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
        //It's hard to build partial search failures since the elasticsearch doesn't consider shard not available exceptions as failures
        //while internally it has exceptions from all shards of a particular shard group, it exposes only the exception on the
        //final shard of the group, the exception for which could be shard not available while a previous failure on a shard of the same
        //group could be outside this category. Since the final exception overrides the exception for the group, it causes inconsistency
        //between the partial search failure and failures post completion.
        if (partialResultsHolder.successfulShardIds.contains(shardIndex)) {
            partialResultsHolder.successfulShardIds.remove(shardIndex);
            partialResultsHolder.successfulShards.decrementAndGet();
        }
    }

    public CompositeSearchProgressActionListener<AsynchronousSearchResponse> searchProgressActionListener() {
        return searchProgressActionListener;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        executor.execute(() -> {
            AsynchronousSearchResponse result;
            try {
                result = successFunction.apply(searchResponse);
                searchProgressActionListener.onResponse(result);
            } catch (Exception ex) {
                searchProgressActionListener.onFailure(ex);
            } finally {
                clearPartialResult();
            }
        });
    }

    @Override
    public void onFailure(Exception e) {
        executor.execute(() -> {
            AsynchronousSearchResponse result;
            try {
                result = failureFunction.apply(e);
                searchProgressActionListener.onResponse(result);
            } catch (Exception ex) {
                searchProgressActionListener.onFailure(ex);
            } finally {
                clearPartialResult();
            }
        });
    }

    /**
     * Invoked once search has completed with response or error.
     */
    private void clearPartialResult() {
        partialResultsHolder = null;
    }

    static class PartialResultsHolder {

        volatile boolean isInitialized;
        final AtomicInteger reducePhase;
        final SetOnce<Integer> totalShards;
        final SetOnce<Integer> skippedShards;
        final SetOnce<SearchResponse.Clusters> clusters;
        final Set<Integer> successfulShardIds;
        final SetOnce<Boolean> hasFetchPhase;
        final AtomicInteger successfulShards;
        final AtomicReference<TotalHits> totalHits;
        final AtomicReference<InternalAggregations> internalAggregations;
        final AtomicReference<InternalAggregations> partialInternalAggregations;
        final long relativeStartMillis;
        final LongSupplier relativeTimeSupplier;
        final Supplier<InternalAggregation.ReduceContextBuilder> reduceContextBuilder;


        PartialResultsHolder(long relativeStartMillis, LongSupplier relativeTimeSupplier,
                             Supplier<InternalAggregation.ReduceContextBuilder> reduceContextBuilder) {
            this.internalAggregations = new AtomicReference<>();
            this.totalShards = new SetOnce<>();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new SetOnce<>();
            this.reducePhase = new AtomicInteger();
            this.isInitialized = false;
            this.hasFetchPhase = new SetOnce<>();
            this.totalHits = new AtomicReference<>();
            this.clusters = new SetOnce<>();
            this.partialInternalAggregations = new AtomicReference<>();
            this.relativeStartMillis = relativeStartMillis;
            this.successfulShardIds = new HashSet<>(1);
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.reduceContextBuilder = reduceContextBuilder;
        }

        public SearchResponse partialResponse() {
            if (isInitialized) {
                SearchHits searchHits = new SearchHits(SearchHits.EMPTY, totalHits.get(), Float.NaN);
                InternalAggregations finalAggregation = null;
                //after final reduce phase this should be present
                if (internalAggregations.get() != null) {
                    finalAggregation = internalAggregations.get();
                    //before final reduce phase ensure we do a top-level final reduce to get reduced aggregation results
                    //else we might be returning back all the partial results aggregated so far
                } else if (partialInternalAggregations.get() != null) {
                    finalAggregation = InternalAggregations.topLevelReduce(Arrays.asList(partialInternalAggregations.get()),
                            reduceContextBuilder.get().forFinalReduction());
                }
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits,
                        finalAggregation, null, null, false, null, reducePhase.get());
                long tookInMillis = relativeTimeSupplier.getAsLong() - relativeStartMillis;
                return new SearchResponse(internalSearchResponse, null, totalShards.get(),
                        successfulShards.get(), skippedShards.get(), tookInMillis, ShardSearchFailure.EMPTY_ARRAY, clusters.get());
            } else {
                return null;
            }
        }
    }
}
