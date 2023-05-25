/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.active;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import org.opensearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import org.opensearch.search.asynchronous.settings.LegacyOpendistroAsynchronousSearchSettings;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;


public class AsynchronousSearchActiveStore {

    private static Logger logger = LogManager.getLogger(AsynchronousSearchActiveStore.class);
    private volatile int nodeConcurrentRunningSearches;
    public static final int NODE_CONCURRENT_RUNNING_SEARCHES = 20;
    public static final Setting<Integer> NODE_CONCURRENT_RUNNING_SEARCHES_SETTING = Setting.intSetting(
            "plugins.asynchronous_search.node_concurrent_running_searches",
            LegacyOpendistroAsynchronousSearchSettings.NODE_CONCURRENT_RUNNING_SEARCHES_SETTING, 0,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ConcurrentMapLong<AsynchronousSearchActiveContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsynchronousSearchActiveStore(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        nodeConcurrentRunningSearches = NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NODE_CONCURRENT_RUNNING_SEARCHES_SETTING,
                this::setNodeConcurrentRunningSearches);
    }

    private void setNodeConcurrentRunningSearches(int nodeConcurrentRunningSearches) {
        this.nodeConcurrentRunningSearches = nodeConcurrentRunningSearches;
    }

    public synchronized void putContext(AsynchronousSearchContextId asynchronousSearchContextId,
                                        AsynchronousSearchActiveContext asynchronousSearchContext,
                                        Consumer<AsynchronousSearchContextId> contextRejectionEventConsumer) {
        if (activeContexts.size() >= nodeConcurrentRunningSearches) {
            contextRejectionEventConsumer.accept(asynchronousSearchContextId);
            throw new OpenSearchRejectedExecutionException("Trying to create too many concurrent searches. Must be less than or equal to: ["
                    + nodeConcurrentRunningSearches + "]. This limit can be set by changing the ["
                    + NODE_CONCURRENT_RUNNING_SEARCHES_SETTING.getKey() + "] settings.");
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
