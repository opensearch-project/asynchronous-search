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

package com.amazon.opendistroforelasticsearch.search.asynchronous.management;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The service takes care of cancelling ongoing searches which have been running past their expiration time and
 * cleaning up asynchronous search responses from disk by scheduling delete-by-query on master to be delegated to the least loaded node
 */
public class AsynchronousSearchManagementService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchManagementService.class);

    private final ClusterService clusterService;
    private final AsynchronousSearchPersistenceService asynchronousSearchPersistenceService;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable activeContextReaperScheduledFuture;
    private static final String RESPONSE_CLEANUP_SCHEDULING_EXECUTOR = ThreadPool.Names.MANAGEMENT;
    private AtomicReference<PersistedResponseCleanUpAndRescheduleRunnable> persistedResponseCleanUpRunnable = new AtomicReference<>();
    private AsynchronousSearchService asynchronousSearchService;
    private TransportService transportService;
    private TimeValue activeContextReaperInterval;
    private TimeValue persistedResponseCleanUpInterval;

    public static final String PERSISTED_RESPONSE_CLEANUP_ACTION_NAME =
            "indices:data/read/opendistro/asynchronous_search/response_cleanup";

    public static final Setting<TimeValue> ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING =
            Setting.timeSetting("opendistro.asynchronous_search.active.context.reaper_interval", TimeValue.timeValueMinutes(5),
                    TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope);
    public static final Setting<TimeValue> PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING =
            Setting.timeSetting("opendistro.asynchronous_search.expired.persisted_response.cleanup_interval",
                    TimeValue.timeValueMinutes(30), TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope);

    @Inject
    public AsynchronousSearchManagementService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                        AsynchronousSearchService asynchronousSearchService, TransportService transportService,
                                        AsynchronousSearchPersistenceService asynchronousSearchPersistenceService) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.asynchronousSearchService = asynchronousSearchService;
        this.transportService = transportService;
        this.asynchronousSearchPersistenceService = asynchronousSearchPersistenceService;
        this.activeContextReaperInterval = ACTIVE_CONTEXT_REAPER_INTERVAL_SETTING.get(settings);
        this.persistedResponseCleanUpInterval = PERSISTED_RESPONSE_CLEAN_UP_INTERVAL_SETTING.get(settings);

        transportService.registerRequestHandler(PERSISTED_RESPONSE_CLEANUP_ACTION_NAME, ThreadPool.Names.SAME, false, false,
                AsynchronousSearchCleanUpRequest::new, new PersistedResponseCleanUpTransportHandler());
    }

    class PersistedResponseCleanUpTransportHandler implements TransportRequestHandler<AsynchronousSearchCleanUpRequest> {

        @Override
        public void messageReceived(AsynchronousSearchCleanUpRequest request, TransportChannel channel, Task task) {
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

    private void asyncCleanUpOperation(AsynchronousSearchCleanUpRequest request, Task task, ActionListener<AcknowledgedResponse> listener) {
        transportService.getThreadPool().executor(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME)
                .execute(() -> performPersistedResponseCleanUpAction(request, listener));
    }

    private void performPersistedResponseCleanUpAction(AsynchronousSearchCleanUpRequest request,
                                                       ActionListener<AcknowledgedResponse> listener) {
        asynchronousSearchPersistenceService.deleteExpiredResponses(listener, request.absoluteTimeInMillis);
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
                AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME);
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
                Set<AsynchronousSearchContext> toFree = asynchronousSearchService.getContextsToReap();
                // don't block on response
                toFree.forEach(
                    context -> asynchronousSearchService.freeContext(context.getAsynchronousSearchId(), context.getContextId(),
                        null, ActionListener.wrap(
                            (response) -> logger.debug("Successfully freed up context [{}] running duration [{}]",
                                context.getAsynchronousSearchId(), context.getExpirationTimeMillis() - context.getStartTimeMillis()),
                            (exception) -> logger.debug(() -> new ParameterizedMessage(
                                "Failed to cleanup asynchronous search context [{}] running duration [{}] due to ",
                                context.getAsynchronousSearchId(),context.getExpirationTimeMillis()
                                    - context.getStartTimeMillis()), exception)
                        )
                    )
                );
            } catch (Exception ex) {
                logger.error("Failed to free up overrunning asynchronous searches due to ", ex);
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
                    .collect(Collectors.toList());
            if (nodes == null || nodes.isEmpty()) {
                logger.debug("Found empty data nodes with asynchronous search enabled attribute [{}] for response clean up", dataNodes);
                return;
            }
            int pos = Randomness.get().nextInt(nodes.size());
            DiscoveryNode randomNode = nodes.get(pos);
            transportService.sendRequest(randomNode, PERSISTED_RESPONSE_CLEANUP_ACTION_NAME,
                    new AsynchronousSearchCleanUpRequest(threadPool.absoluteTimeInMillis()),
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
                            return AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME;
                        }
                    });

        } catch (Exception ex) {
            logger.error("Failed to schedule asynchronous search cleanup", ex);
        }
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
            final boolean shutDown = e instanceof OpenSearchRejectedExecutionException && ((OpenSearchRejectedExecutionException) e)
                    .isExecutorShutdown();
            logger.log(shutDown ? Level.DEBUG : Level.WARN, "asynchronous search clean up job rejected [{}]", reason, e);
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


    static class AsynchronousSearchCleanUpRequest extends ActionRequest {

        private final long absoluteTimeInMillis;

        AsynchronousSearchCleanUpRequest(long absoluteTimeInMillis) {
            this.absoluteTimeInMillis = absoluteTimeInMillis;
        }

        AsynchronousSearchCleanUpRequest(StreamInput in) throws IOException {
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
         * The reason for deleting expired asynchronous searches.
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
            AsynchronousSearchCleanUpRequest asynchronousSearchCleanUpRequest = (AsynchronousSearchCleanUpRequest) o;
            return absoluteTimeInMillis == asynchronousSearchCleanUpRequest.absoluteTimeInMillis;
        }

        @Override
        public String toString() {
            return "[expirationTimeMillis] : " + absoluteTimeInMillis;
        }
    }
}
