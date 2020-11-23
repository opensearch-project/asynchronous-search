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

package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShard;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;


/***
 * The implementation of {@link CompositeSearchProgressActionListener} responsible for updating the partial results of a single async
 * search request. All partial results are updated atomically.
 */
public class AsyncSearchProgressListener extends SearchProgressActionListener {

    private final PartialResultsHolder partialResultsHolder;
    private final CompositeSearchProgressActionListener<AsyncSearchResponse> searchProgressActionListener;
    private final CheckedFunction<SearchResponse, AsyncSearchResponse, IOException> successFunction;
    private final CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction;
    private final ExecutorService executor;

    public AsyncSearchProgressListener(long relativeStartMillis, CheckedFunction<SearchResponse, AsyncSearchResponse,
            IOException> successFunction, CheckedFunction<Exception, AsyncSearchResponse, IOException> failureFunction,
                                       ExecutorService executor, LongSupplier relativeTimeSupplier) {
        this.successFunction = successFunction;
        this.failureFunction = failureFunction;
        this.executor = executor;
        this.partialResultsHolder = new PartialResultsHolder(relativeStartMillis, relativeTimeSupplier);
        this.searchProgressActionListener = new CompositeSearchProgressActionListener<AsyncSearchResponse>();
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
        partialResultsHolder.clusters.set(clusters);
        partialResultsHolder.isInitialized.set(true);
        partialResultsHolder.shards.set(shards);
    }

    @Override
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                   DelayableWriteable.Serialized<InternalAggregations> aggs, int reducePhase) {
        assert reducePhase > partialResultsHolder.reducePhase.get() : "reduce phase "+ reducePhase + "less than previous phase"
                + partialResultsHolder.reducePhase.get();
        partialResultsHolder.delayedInternalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        assert reducePhase > partialResultsHolder.reducePhase.get() : "reduce phase "+ reducePhase + "less than previous phase"
                + partialResultsHolder.reducePhase.get();
        partialResultsHolder.internalAggregations.set(aggs);
        partialResultsHolder.reducePhase.set(reducePhase);
        partialResultsHolder.totalHits.set(totalHits);
    }

    @Override
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert partialResultsHolder.hasFetchPhase.get() : "Fetch failure without fetch phase";
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onFetchResult(int shardIndex) {
        assert partialResultsHolder.hasFetchPhase.get() : "Fetch result without fetch phase";
        assert shardIndex < partialResultsHolder.totalShards.get();
        partialResultsHolder.successfulShards.incrementAndGet();
    }

    @Override
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        onSearchFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected void onQueryResult(int shardIndex) {
        assert shardIndex < partialResultsHolder.totalShards.get();
        // query and fetch optimization for single shard
        if (partialResultsHolder.hasFetchPhase.get() == false || partialResultsHolder.totalShards.get() == 1) {
            partialResultsHolder.successfulShards.incrementAndGet();
        }
    }

    private void onSearchFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
        AtomicArray<ShardSearchFailure> shardFailures = partialResultsHolder.shardFailures.get();
        // lazily create shard failures, so we can early build the empty shard failure list in most cases (no failures)
        if (shardFailures == null) { // this is double checked locking but it's fine since SetOnce uses a volatile read internally
            synchronized (partialResultsHolder.shardFailuresMutex) {
                shardFailures = this.partialResultsHolder.shardFailures.get(); // read again otherwise somebody else has created it?
                if (shardFailures == null) { // still null so we are the first and create a new instance
                    shardFailures = new AtomicArray<>(partialResultsHolder.totalShards.get());
                    this.partialResultsHolder.shardFailures.set(shardFailures);
                }
                shardFailures.setOnce(shardIndex, new ShardSearchFailure(e, shardTarget));
            }
        } else {
            shardFailures.setOnce(shardIndex, new ShardSearchFailure(e, shardTarget));
        }
    }

    public CompositeSearchProgressActionListener<AsyncSearchResponse> searchProgressActionListener() {
        return searchProgressActionListener;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        executor.execute(() -> {
            AsyncSearchResponse result;
            try {
                result = successFunction.apply(searchResponse);
                searchProgressActionListener.onResponse(result);
            } catch (Exception ex) {
                searchProgressActionListener.onFailure(ex);
            }
        });
    }

    @Override
    public void onFailure(Exception e) {
        executor.execute(() -> {
            AsyncSearchResponse result;
            try {
                result = failureFunction.apply(e);
                searchProgressActionListener.onResponse(result);
            } catch (Exception ex) {
                searchProgressActionListener.onFailure(ex);
            }
        });
    }

    static class PartialResultsHolder {
        final AtomicInteger reducePhase;
        final SetOnce<Boolean> isInitialized;
        final SetOnce<Integer> totalShards;
        final SetOnce<Integer> skippedShards;
        final SetOnce<SearchResponse.Clusters> clusters;
        final SetOnce<List<SearchShard>> shards;
        final SetOnce<Boolean> hasFetchPhase;
        final SetOnce<AtomicArray<ShardSearchFailure>> shardFailures;
        final AtomicInteger successfulShards;
        final AtomicReference<TotalHits> totalHits;
        final AtomicReference<InternalAggregations> internalAggregations;
        final AtomicReference<DelayableWriteable.Serialized<InternalAggregations>> delayedInternalAggregations;
        final long relativeStartMillis;
        final LongSupplier relativeTimeSupplier;
        final Object shardFailuresMutex;


        PartialResultsHolder(long relativeStartMillis, LongSupplier relativeTimeSupplier) {
            this.internalAggregations = new AtomicReference<>();
            this.totalShards = new SetOnce<>();
            this.successfulShards = new AtomicInteger();
            this.skippedShards = new SetOnce<>();
            this.reducePhase = new AtomicInteger(-1);
            this.isInitialized = new SetOnce<>();
            this.hasFetchPhase = new SetOnce<>();
            this.totalHits = new AtomicReference<>();
            this.clusters = new SetOnce<>();
            this.delayedInternalAggregations = new AtomicReference<>();
            this.relativeStartMillis = relativeStartMillis;
            this.shards = new SetOnce<>();
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.shardFailures = new SetOnce<>();
            this.shardFailuresMutex = new Object();
        }

        public SearchResponse partialResponse() {
            if (isInitialized.get()) {
                SearchHits searchHits = new SearchHits(SearchHits.EMPTY, totalHits.get(), Float.NaN);
                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(searchHits,
                        internalAggregations.get() == null ? (delayedInternalAggregations.get() != null
                                ? delayedInternalAggregations.get().expand() : null) : internalAggregations.get(),
                        null, null, false, null, reducePhase.get());
                long tookInMillis = relativeTimeSupplier.getAsLong() - relativeStartMillis;
                return new SearchResponse(internalSearchResponse, null, totalShards.get(),
                        successfulShards.get(), skippedShards.get(), tookInMillis, buildShardFailures(), clusters.get());
            } else {
                return null;
            }
        }

        ShardSearchFailure[] buildShardFailures() {
            AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures.get();
            if (shardFailures == null) {
                return ShardSearchFailure.EMPTY_ARRAY;
            }
            List<ShardSearchFailure> entries = shardFailures.asList();
            ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
            for (int i = 0; i < failures.length; i++) {
                failures[i] = entries.get(i);
            }
            return failures;
        }
    }
}
