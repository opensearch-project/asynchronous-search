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

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchDeletedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;


public class AsyncSearchActiveStore {

    private static Logger logger = LogManager.getLogger(AsyncSearchActiveStore.class);
    private volatile int maxRunningSearches;
    public static final int DEFAULT_MAX_RUNNING_SEARCHES = 20;
    public static final Setting<Integer> MAX_RUNNING_SEARCHES_SETTING = Setting.intSetting(
            "opendistro_asynchronous_search.max_running_searches", DEFAULT_MAX_RUNNING_SEARCHES, 0, Setting.Property.Dynamic,
            Setting.Property.NodeScope);

    private final ConcurrentMapLong<AsyncSearchActiveContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsyncSearchActiveStore(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        maxRunningSearches = MAX_RUNNING_SEARCHES_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_RUNNING_SEARCHES_SETTING, this::setMaxRunningSearches);
    }

    private void setMaxRunningSearches(int maxRunningSearches) {
        this.maxRunningSearches = maxRunningSearches;
    }

    public synchronized void putContext(AsyncSearchContextId asyncSearchContextId, AsyncSearchActiveContext asyncSearchContext,
                                        Consumer<AsyncSearchContextId> contextRejectionEventConsumer) {
        if (activeContexts.size() >= maxRunningSearches) {
            contextRejectionEventConsumer.accept(asyncSearchContextId);
            throw new EsRejectedExecutionException("Trying to create too many running contexts. Must be less than or equal to: ["
                    + maxRunningSearches + "]. This limit can be set by changing the [" + MAX_RUNNING_SEARCHES_SETTING.getKey()
                    + "] setting.");
        }
        activeContexts.put(asyncSearchContextId.getId(), asyncSearchContext);
    }

    public Optional<AsyncSearchActiveContext> getContext(AsyncSearchContextId contextId) {
        AsyncSearchActiveContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return Optional.empty();
        }
        if (context.getContextId().getContextId().equals(contextId.getContextId())) {
            return Optional.of(context);
        }
        return Optional.empty();
    }


    public Map<Long, AsyncSearchActiveContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }

    /**
     * Should strictly be executed from within the state machine for a {@link SearchDeletedEvent}
     *
     * @param asyncSearchContextId the context Id to be DELETED
     * @return if the context could be DELETED
     */
    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) {
        //TODO assert calledFromAsyncSearchStateMachine() : "Method should only ever be invoked by the state machine";
        AsyncSearchActiveContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.debug("Removing async search [{}] from active store", asyncSearchContext.getAsyncSearchId());
            asyncSearchContext.close();
            activeContexts.remove(asyncSearchContextId.getId());
            return true;
        }
        return false;
    }

    private static boolean calledFromAsyncSearchStateMachine() {
        List<StackTraceElement> frames = Stream.of(Thread.currentThread().getStackTrace()).
                skip(1). //skip getStackTrace
                limit(10). //limit depth of analysis to 10 frames, it should be enough
                filter(f ->
                {
                    try {
                        return AsyncSearchStateMachine.class.isAssignableFrom(Class.forName(f.getClassName()));
                    } catch (Exception ignored) {
                        return false;
                    }
                }
        ).
                collect(Collectors.toList());
        //the list should contain trigger method of the state machine
        return frames.stream().anyMatch(f -> f.getMethodName().equals("trigger"));
    }
}
