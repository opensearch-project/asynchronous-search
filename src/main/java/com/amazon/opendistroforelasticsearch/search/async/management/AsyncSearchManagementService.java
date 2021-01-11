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

package com.amazon.opendistroforelasticsearch.search.async.management;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The service takes care of cancelling ongoing searches which have been running past their expiration time and cleaning up async search
 * responses from disk by scheduling delete-by-query on master to be delegated to the least loaded node
 */
public class AsyncSearchManagementService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchManagementService.class);

    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable activeContextReaperScheduledFuture;
    private static final String RESPONSE_CLEANUP_SCHEDULING_EXECUTOR = ThreadPool.Names.MANAGEMENT;
    private AtomicReference<PersistedResponseCleanUpAndRescheduleRunnable> persistedResponseCleanUpRunnable = new AtomicReference<>();
    private AsyncSearchService asyncSearchService;
    private TransportService transportService;
    private TimeValue activeContextReaperInterval;
    private TimeValue persistedResponseCleanUpInterval;

    public static final String PERSISTED_RESPONSE_CLEANUP_ACTION_NAME =
            "indices:data/read/opendistro/asynchronous_search/response_cleanup";

    public static final Setting<TimeValue> ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING =
            Setting.timeSetting("opendistro_asynchronous_search.active.context.reaper_interval", TimeValue.timeValueMinutes(5),
                    TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope);
    public static final Setting<TimeValue> PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING =
            Setting.timeSetting("opendistro_asynchronous_search.expired.persisted_response.cleanup_interval",
                    TimeValue.timeValueMinutes(5), TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope);

    @Inject
    public AsyncSearchManagementService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                        AsyncSearchService asyncSearchService, TransportService transportService,
                                        AsyncSearchPersistenceService asyncSearchPersistenceService) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.asyncSearchService = asyncSearchService;
        this.transportService = transportService;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.activeContextReaperInterval = ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING.get(settings);
        this.persistedResponseCleanUpInterval = PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(settings);

        transportService.registerRequestHandler(PERSISTED_RESPONSE_CLEANUP_ACTION_NAME, ThreadPool.Names.SAME, false, false,
                AsyncSearchCleanUpRequest::new, new PersistedResponseCleanUpTransportHandler());
    }

    class PersistedResponseCleanUpTransportHandler implements TransportRequestHandler<AsyncSearchCleanUpRequest> {

        @Override
        public void messageReceived(AsyncSearchCleanUpRequest request, TransportChannel channel, Task task) {
            asyncCleanUpOperation(request, task, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (IOException ex) {
                    logger.warn(() -> new ParameterizedMessage(
                            "Failed to send cleanup error response for request [{}]", request), ex);
                }
            }));
        }
    }

    private void asyncCleanUpOperation(AsyncSearchCleanUpRequest request, Task task, ActionListener<AcknowledgedResponse> listener) {
        transportService.getThreadPool().executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME)
                .execute(() -> performPersistedResponseCleanUpAction(request, listener));
    }

    private void performPersistedResponseCleanUpAction(AsyncSearchCleanUpRequest request, ActionListener<AcknowledgedResponse> listener) {
        asyncSearchPersistenceService.deleteExpiredResponses(listener, request.absoluteTimeInMillis);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() && persistedResponseCleanUpRunnable.get() == null) {
            logger.trace("elected as master, triggering response cleanup tasks");
            triggerCleanUp(event.state(), "became master");

            final PersistedResponseCleanUpAndRescheduleRunnable newRunnable = new PersistedResponseCleanUpAndRescheduleRunnable();
            persistedResponseCleanUpRunnable.set(newRunnable);
            threadPool.scheduleUnlessShuttingDown(persistedResponseCleanUpInterval, RESPONSE_CLEANUP_SCHEDULING_EXECUTOR, newRunnable);
        } else if (event.localNodeMaster() == false) {
            persistedResponseCleanUpRunnable.set(null);
            return;
        }
    }

    private void triggerCleanUp(ClusterState clusterState, String reason) {
        if (clusterState.nodes().getDataNodes().size() > 0) {
            logger.debug("triggering response cleanup in background [{}]", reason);
            threadPool.executor(RESPONSE_CLEANUP_SCHEDULING_EXECUTOR).execute(new ResponseCleanUpRunnable(reason));
        }
    }

    @Override
    protected void doStart() {
        activeContextReaperScheduledFuture = threadPool.scheduleWithFixedDelay(new ActiveContextReaper(), activeContextReaperInterval,
                AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
    }

    @Override
    protected void doStop() {
        persistedResponseCleanUpRunnable.set(null);
        activeContextReaperScheduledFuture.cancel();
    }

    @Override
    protected void doClose() {
        persistedResponseCleanUpRunnable.set(null);
        activeContextReaperScheduledFuture.cancel();
    }

    class ActiveContextReaper implements Runnable {

        @Override
        public void run() {
            try {
                Set<AsyncSearchContext> toFree = asyncSearchService.getContextsToReap();
                // don't block on response
                toFree.forEach(
                    context -> asyncSearchService.freeContext(context.getAsyncSearchId(), context.getContextId(),
                        null, ActionListener.wrap(
                            (response) -> logger.debug("Successfully freed up context [{}] running duration [{}]",
                                context.getAsyncSearchId(), context.getExpirationTimeMillis() - context.getStartTimeMillis()),
                            (exception) -> logger.debug(() -> new ParameterizedMessage(
                                "Failed to cleanup async search context [{}] running duration [{}] due to ",
                                context.getAsyncSearchId(),context.getExpirationTimeMillis() - context.getStartTimeMillis()), exception)
                        )
                    )
                );
            } catch (Exception ex) {
                logger.error("Failed to free up overrunning async searches due to ", ex);
            }
        }
    }

    public final void performCleanUp() {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            ImmutableOpenMap<String, DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
            List<DiscoveryNode> nodes = Stream.of(dataNodes.values().toArray(DiscoveryNode.class))
                    .filter((node) -> isAsyncSearchEnabledNode(node)).collect(Collectors.toList());
            if (nodes == null || nodes.isEmpty()) {
                logger.debug("Found empty data nodes with async search enabled attribute [{}] for response clean up", dataNodes);
                return;
            }
            int pos = Randomness.get().nextInt(nodes.size());
            DiscoveryNode randomNode = nodes.get(pos);
            transportService.sendRequest(randomNode, PERSISTED_RESPONSE_CLEANUP_ACTION_NAME,
                    new AsyncSearchCleanUpRequest(threadPool.absoluteTimeInMillis()),
                    new TransportResponseHandler<AcknowledgedResponse>() {

                        @Override
                        public AcknowledgedResponse read(StreamInput in) throws IOException {
                            return new AcknowledgedResponse(in);
                        }

                        @Override
                        public void handleResponse(AcknowledgedResponse response) {
                            logger.debug("Successfully executed clean up action on node [{}] with response [{}]", randomNode,
                                    response.isAcknowledged());
                        }

                        @Override
                        public void handleException(TransportException e) {
                            logger.error(() -> new ParameterizedMessage("Exception executing action [{}]",
                                    PERSISTED_RESPONSE_CLEANUP_ACTION_NAME), e);
                        }

                        @Override
                        public String executor() {
                            return AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
                        }
                    });

        } catch (Exception ex) {
            logger.error("Failed to schedule async search cleanup", ex);
        }
    }

    // TODO: only here temporarily for BWC development, remove once complete
    private boolean isAsyncSearchEnabledNode(DiscoveryNode discoveryNode) {
        return Booleans.isTrue(discoveryNode.getAttributes().getOrDefault("asynchronous_search_enabled", "true"));
    }


    private class ResponseCleanUpRunnable extends AbstractRunnable {
        private final String reason;

        ResponseCleanUpRunnable(String reason) {
            this.reason = reason;
        }

        @Override
        protected void doRun() {
            performCleanUp();
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(new ParameterizedMessage("sync search clean up job failed [{}]", reason), e);
        }


        @Override
        public void onRejection(Exception e) {
            final boolean shutDown = e instanceof EsRejectedExecutionException && ((EsRejectedExecutionException) e).isExecutorShutdown();
            logger.log(shutDown ? Level.DEBUG : Level.WARN, "async search clean up job rejected [{}]", reason, e);
        }
    }


    private class PersistedResponseCleanUpAndRescheduleRunnable extends ResponseCleanUpRunnable {
        PersistedResponseCleanUpAndRescheduleRunnable() {
            super("scheduled");
        }

        @Override
        protected void doRun() {
            if (this == persistedResponseCleanUpRunnable.get()) {
                super.doRun();
            } else {
                logger.trace("master changed, scheduled cleanup job is stale");
            }
        }

        @Override
        public void onAfter() {
            if (this == persistedResponseCleanUpRunnable.get()) {
                logger.trace("scheduling next clean up job in [{}]", persistedResponseCleanUpInterval);
                threadPool.scheduleUnlessShuttingDown(persistedResponseCleanUpInterval, RESPONSE_CLEANUP_SCHEDULING_EXECUTOR, this);
            }
        }
    }


    static class AsyncSearchCleanUpRequest extends ActionRequest {

        private final long absoluteTimeInMillis;

        AsyncSearchCleanUpRequest(long absoluteTimeInMillis) {
            this.absoluteTimeInMillis = absoluteTimeInMillis;
        }

        AsyncSearchCleanUpRequest(StreamInput in) throws IOException {
            super(in);
            this.absoluteTimeInMillis = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(absoluteTimeInMillis);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * The reason for deleting expired async searches.
         */
        public long getAbsoluteTimeInMillis() {
            return absoluteTimeInMillis;
        }


        @Override
        public int hashCode() {
            return Objects.hash(absoluteTimeInMillis);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            AsyncSearchCleanUpRequest asyncSearchCleanUpRequest = (AsyncSearchCleanUpRequest) o;
            return absoluteTimeInMillis == asyncSearchCleanUpRequest.absoluteTimeInMillis;
        }

        @Override
        public String toString() {
            return "[expirationTimeMillis] : " + absoluteTimeInMillis;
        }
    }
}
