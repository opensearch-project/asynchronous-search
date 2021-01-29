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

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;


public class AsynchronousSearchActiveStore {

    private static Logger logger = LogManager.getLogger(AsynchronousSearchActiveStore.class);
    private volatile int maxRunningSearches;
    public static final int DEFAULT_MAX_RUNNING_SEARCHES = 20;
    public static final Setting<Integer> MAX_RUNNING_SEARCHES_SETTING = Setting.intSetting(
            "opendistro_asynchronous_search.max_running_searches", DEFAULT_MAX_RUNNING_SEARCHES, 0, Setting.Property.Dynamic,
            Setting.Property.NodeScope);

    private final ConcurrentMapLong<AsynchronousSearchActiveContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsynchronousSearchActiveStore(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        maxRunningSearches = MAX_RUNNING_SEARCHES_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_RUNNING_SEARCHES_SETTING, this::setMaxRunningSearches);
    }

    private void setMaxRunningSearches(int maxRunningSearches) {
        this.maxRunningSearches = maxRunningSearches;
    }

    public synchronized void putContext(AsynchronousSearchContextId asynchronousSearchContextId,
                                        AsynchronousSearchActiveContext asynchronousSearchContext,
                                        Consumer<AsynchronousSearchContextId> contextRejectionEventConsumer) {
        if (activeContexts.size() >= maxRunningSearches) {
            contextRejectionEventConsumer.accept(asynchronousSearchContextId);
            throw new EsRejectedExecutionException("Trying to create too many running contexts. Must be less than or equal to: ["
                    + maxRunningSearches + "]. This limit can be set by changing the [" + MAX_RUNNING_SEARCHES_SETTING.getKey()
                    + "] setting.");
        }
        activeContexts.put(asynchronousSearchContextId.getId(), asynchronousSearchContext);
    }

    public Optional<AsynchronousSearchActiveContext> getContext(AsynchronousSearchContextId contextId) {
        AsynchronousSearchActiveContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return Optional.empty();
        }
        if (context.getContextId().getContextId().equals(contextId.getContextId())) {
            return Optional.of(context);
        }
        return Optional.empty();
    }


    public Map<Long, AsynchronousSearchActiveContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }

    /**
     * Should strictly be executed from within the state machine for a {@link SearchDeletedEvent}
     *
     * @param asynchronousSearchContextId the context Id to be DELETED
     * @return if the context could be DELETED
     */
    public boolean freeContext(AsynchronousSearchContextId asynchronousSearchContextId) {
        assert calledFromAsynchronousSearchStateMachine() : "Method should only ever be invoked by the state machine";
        AsynchronousSearchActiveContext asynchronousSearchContext = activeContexts.get(asynchronousSearchContextId.getId());
        if (asynchronousSearchContext != null) {
            logger.debug("Removing asynchronous search [{}] from active store", asynchronousSearchContext.getAsynchronousSearchId());
            asynchronousSearchContext.close();
            activeContexts.remove(asynchronousSearchContextId.getId());
            return true;
        }
        return false;
    }

    private static boolean calledFromAsynchronousSearchStateMachine() {
        return Stream.of(Thread.currentThread().getStackTrace()).
                skip(1). //skip getStackTrace
                limit(10). //limit depth of analysis to 10 frames, it should be enough
                anyMatch(f ->
                {
                    try {
                        boolean isTestMethodInvocation = f.getClassName().contains("AsynchronousSearchActiveStoreTests");
                        boolean isStateMachineTriggerMethodInvocation = AsynchronousSearchStateMachine.class
                                .isAssignableFrom(Class.forName(f.getClassName())) && f.getMethodName().equals("trigger");
                        return isTestMethodInvocation || isStateMachineTriggerMethodInvocation;
                    } catch (Exception ignored) {
                        return false;
                    }
                }
        );
    }
}
