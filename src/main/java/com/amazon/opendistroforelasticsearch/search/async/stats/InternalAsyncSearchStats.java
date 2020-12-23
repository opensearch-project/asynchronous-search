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

package com.amazon.opendistroforelasticsearch.search.async.stats;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.metrics.CounterMetric;

public class InternalAsyncSearchStats implements AsyncSearchContextListener {

    private final CountStatsHolder countStatsHolder = new CountStatsHolder();

    @Override
    public void onContextFailed(AsyncSearchContextId contextId) {
        countStatsHolder.failedAsyncSearchCount.inc();
        countStatsHolder.runningAsyncSearchCount.dec();
    }

    @Override
    public void onContextPersisted(AsyncSearchContextId asyncSearchContextId) {
        countStatsHolder.persistedAsyncSearchCount.inc();
    }

    @Override
    public void onContextPersistFailed(AsyncSearchContextId contextId) {
        countStatsHolder.persistFailedAsyncSearchCount.inc();
    }

    @Override
    public void onContextRunning(AsyncSearchContextId context) {
        countStatsHolder.runningAsyncSearchCount.inc();
    }

    @Override
    public void onContextRejected(AsyncSearchContextId contextId) {
        countStatsHolder.rejectedAsyncSearchCount.inc();
    }

    @Override
    public void onRunningContextClosed(AsyncSearchContextId contextId) {
        countStatsHolder.runningAsyncSearchCount.dec();
    }

    @Override
    public void onContextCompleted(AsyncSearchContextId context) {
        countStatsHolder.completedAsyncSearchCount.inc();
        countStatsHolder.runningAsyncSearchCount.dec();
    }

    public AsyncSearchStats stats(DiscoveryNode node) {
        return new AsyncSearchStats(node, countStatsHolder.countStats());
    }

    static final class CountStatsHolder {
        final CounterMetric runningAsyncSearchCount = new CounterMetric();
        final CounterMetric persistedAsyncSearchCount = new CounterMetric();
        final CounterMetric persistFailedAsyncSearchCount = new CounterMetric();
        final CounterMetric failedAsyncSearchCount = new CounterMetric();
        final CounterMetric completedAsyncSearchCount = new CounterMetric();
        final CounterMetric rejectedAsyncSearchCount = new CounterMetric();

        public AsyncSearchCountStats countStats() {
            return new AsyncSearchCountStats(runningAsyncSearchCount.count(), persistedAsyncSearchCount.count(),
                    completedAsyncSearchCount.count(), failedAsyncSearchCount.count(), rejectedAsyncSearchCount.count(),
                    persistFailedAsyncSearchCount.count());
        }
    }
}
