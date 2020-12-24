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

package com.amazon.opendistroforelasticsearch.search.async.service;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchTransition;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchClosedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.processor.AsyncSearchPostProcessor;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.InternalAsyncSearchStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.async.UserAuthUtils.isUserValid;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSISTING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState.SUCCEEDED;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/***
 * Manages the lifetime of {@link AsyncSearchContext} for all the async searches running on the coordinator node.
 */

public class AsyncSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsyncSearchService.class);

    public static final Setting<TimeValue> MAX_KEEP_ALIVE_SETTING = Setting.positiveTimeSetting("async_search.max_keep_alive",
            timeValueDays(10), Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
            "async_search.max_wait_for_completion_timeout", timeValueMinutes(1), Setting.Property.NodeScope,
            Setting.Property.Dynamic);

    private volatile long maxKeepAlive;
    private volatile long maxWaitForCompletion;
    private final AtomicLong idGenerator = new AtomicLong();
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService persistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final AsyncSearchPostProcessor asyncSearchPostProcessor;
    private final LongSupplier currentTimeSupplier;
    private final AsyncSearchStateMachine asyncSearchStateMachine;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final InternalAsyncSearchStats statsListener;

    public AsyncSearchService(AsyncSearchPersistenceService asyncSearchPersistenceService, AsyncSearchActiveStore asyncSearchActiveStore,
                              Client client, ClusterService clusterService, ThreadPool threadPool,
                              NamedWriteableRegistry namedWriteableRegistry) {
        this.statsListener = new InternalAsyncSearchStats();
        this.client = client;
        Settings settings = clusterService.getSettings();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_KEEP_ALIVE_SETTING, this::setKeepAlive);
        setKeepAlive(MAX_KEEP_ALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                this::setMaxWaitForCompletion);
        setMaxWaitForCompletion(MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(settings));
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.persistenceService = asyncSearchPersistenceService;
        this.currentTimeSupplier = System::currentTimeMillis;
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchStateMachine = initStateMachine();
        this.asyncSearchPostProcessor = new AsyncSearchPostProcessor(persistenceService, asyncSearchActiveStore, asyncSearchStateMachine,
                this::freeActiveContext);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    private void setMaxWaitForCompletion(TimeValue maxWaitForCompletion) {
        this.maxWaitForCompletion = maxWaitForCompletion.millis();
    }

    private void setKeepAlive(TimeValue maxKeepAlive) {
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    /**
     * Creates a new active async search for a newly submitted async search.
     *
     * @param request                 the SubmitAsyncSearchRequest
     * @param relativeStartTimeMillis the relative start time of the search in millis
     * @param user current user
     * @param reduceContextBuilder    the reference for the reduceContextBuilder
     * @return the AsyncSearchContext for the submitted request
     */
    public AsyncSearchContext createAndStoreContext(SubmitAsyncSearchRequest request, long relativeStartTimeMillis,
                                                    Supplier<InternalAggregation.ReduceContextBuilder> reduceContextBuilder, User user) {
        validateRequest(request);
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), idGenerator.incrementAndGet());
        AsyncSearchProgressListener progressActionListener = new AsyncSearchProgressListener(relativeStartTimeMillis,
                (response) -> asyncSearchPostProcessor.processSearchResponse(response, asyncSearchContextId),
                (e) -> asyncSearchPostProcessor.processSearchFailure(e, asyncSearchContextId),
                threadPool.executor(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME), threadPool::relativeTimeInMillis,
                reduceContextBuilder);
        AsyncSearchActiveContext asyncSearchContext = new AsyncSearchActiveContext(asyncSearchContextId, clusterService.localNode().getId(),
                request.getKeepAlive(), request.getKeepOnCompletion(), threadPool, currentTimeSupplier, progressActionListener,
                statsListener, user);
        asyncSearchActiveStore.putContext(asyncSearchContextId, asyncSearchContext, statsListener::onContextRejected);
        return asyncSearchContext;
    }

    /**
     * Stores information of the {@linkplain SearchTask} in the async search and signals start of the the underlying
     * {@linkplain SearchAction}
     *
     * @param searchTask           The {@linkplain SearchTask} which stores information of the currently running {@linkplain SearchTask}
     * @param asyncSearchContextId the id of the active asyncsearch context
     */
    public void bootstrapSearch(SearchTask searchTask, AsyncSearchContextId asyncSearchContextId) {
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext context = asyncSearchContextOptional.get();
            asyncSearchStateMachine.trigger(new SearchStartedEvent(context, searchTask));
        }
    }

    /**
     * Tries to find an {@linkplain AsyncSearchActiveContext}. If not found, queries the {@linkplain AsyncSearchPersistenceService}  for
     * a hit. If a response is found, it builds and returns an {@linkplain AsyncSearchPersistenceContext}, else throws
     * {@linkplain ResourceNotFoundException}
     *
     * @param id                   The async search id
     * @param asyncSearchContextId the Async search context id
     * @param user                 current user
     * @param listener             to be invoked on finding an {@linkplain AsyncSearchContext}
     */
    public void findContext(String id, AsyncSearchContextId asyncSearchContextId, User user, ActionListener<AsyncSearchContext> listener) {
        Optional<AsyncSearchActiveContext> optionalAsyncSearchActiveContext = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (optionalAsyncSearchActiveContext.isPresent()) {
            logger.debug("Active context is present for async search ID [{}]", id);
            AsyncSearchActiveContext asyncSearchActiveContext = optionalAsyncSearchActiveContext.get();
            if (isUserValid(user, asyncSearchActiveContext.getUser()) == false) {
                logger.debug("Invalid user requesting GET active context for async search id {}", id);
                listener.onFailure(new ElasticsearchSecurityException(
                        "User doesn't have necessary roles to access the async search with id "+ id, RestStatus.FORBIDDEN));
            } else {
                listener.onResponse(asyncSearchActiveContext);
            }
        } else {
            logger.debug("Active context is not present for async search ID [{}]", id);
            persistenceService.getResponse(id, user, ActionListener.wrap(
                    (persistenceModel) ->
                            listener.onResponse(new AsyncSearchPersistenceContext(id, asyncSearchContextId, persistenceModel,
                            currentTimeSupplier, namedWriteableRegistry)),
                    ex -> {
                        logger.debug(() -> new ParameterizedMessage("Context not found for ID  in the system index {}", id), ex);
                        listener.onFailure(ex);
                    }
            ));
        }
    }

    public Map<Long, AsyncSearchActiveContext> getAllActiveContexts() {
        return asyncSearchActiveStore.getAllContexts();
    }

    public Set<AsyncSearchContext> getContextsToReap() {
        Map<Long, AsyncSearchActiveContext> allContexts = asyncSearchActiveStore.getAllContexts();
        return Collections.unmodifiableSet(allContexts.values().stream()
                .filter(Objects::nonNull)
                .filter((c) -> EnumSet.of(CLOSED, PERSIST_FAILED).contains(c.getAsyncSearchState()) || c.isExpired())
                .collect(Collectors.toSet()));
    }


    /**
     * Attempts to find both an {@linkplain AsyncSearchActiveContext} and an {@linkplain AsyncSearchPersistenceContext} and delete them.
     * If at least one of the aforementioned objects are found and deleted successfully, the listener is invoked with #true, else
     * {@linkplain ResourceNotFoundException} is thrown.
     *
     * @param id                   async search id
     * @param asyncSearchContextId context id
     * @param user                 current user
     * @param listener             listener to invoke on deletion or failure to do so
     */
    public void freeContext(String id, AsyncSearchContextId asyncSearchContextId, User user, ActionListener<Boolean> listener) {
        // if there are no context found to be cleaned up we throw a ResourceNotFoundException
        GroupedActionListener<Boolean> groupedDeletionListener = new GroupedActionListener<>(
                ActionListener.wrap((responses) -> {
                    if (responses.stream().anyMatch(r -> r)) {
                        logger.debug("Free context for async search id {} successful ", id);
                        listener.onResponse(true);
                    } else {
                        logger.debug("Freeing context, async search id {} not found ", id);
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
                }, listener::onFailure), 2);
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        if (asyncSearchContextOptional.isPresent()) {
            logger.warn("Active context present for async search id [{}]", id);
            AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
            if(isUserValid(user, asyncSearchContext.getUser()) == false) {
                groupedDeletionListener.onFailure(new ElasticsearchSecurityException(
                        "User doesn't have necessary roles to access the async search with id "+ id, RestStatus.FORBIDDEN));
            } else {
                cancelAndFreeActiveAndPersistedContext(asyncSearchContext, groupedDeletionListener);
            }
        } else {
            logger.warn("Active context NOT present for async search id [{}]", id);
            // async search context didn't exist so obviously we didn't delete
            groupedDeletionListener.onResponse(false);
            //deleted persisted context if one exists. If not the listener returns acknowledged as false
            //we don't need to acquire lock if the in-memory context doesn't exist. For persistence context we have a distributed view
            //with the last writer wins policy
            logger.warn("Deleting async search id [{}] from system index ", id);
            persistenceService.deleteResponse(id, user, groupedDeletionListener);
        }
    }

    private void cancelTask(AsyncSearchActiveContext asyncSearchContext, String reason) {
        if (asyncSearchContext.getTask() != null && asyncSearchContext.getTask().isCancelled() == false) {
            try {
                CancelTasksRequest cancelTasksRequest = new CancelTasksRequest()
                        .setTaskId(new TaskId(clusterService.localNode().getId(), asyncSearchContext.getTask().getId())).setReason(reason);
                CancelTasksResponse cancelTasksResponse = client.admin().cluster().cancelTasks(cancelTasksRequest).actionGet();
                logger.debug("Successfully cancelled tasks [{}] with async search id [{}] with response [{}]",
                        asyncSearchContext.getTask(), asyncSearchContext.getAsyncSearchId(), cancelTasksResponse);
            } catch (Exception ex) {
                logger.error(() -> new ParameterizedMessage("Unable to cancel async search task [{}] " +
                        "for async search id [{}]", asyncSearchContext.getTask(), asyncSearchContext.getAsyncSearchId()), ex);
            }
        }

    }

    // We are skipping user check in this while deleting from the persisted layer
    // as we have already checked for user in the present active context.
    private void cancelAndFreeActiveAndPersistedContext(AsyncSearchActiveContext asyncSearchContext,
                                                        GroupedActionListener<Boolean> groupedDeletionListener) {
        //Intent of the lock here is to disallow ongoing migration to system index
        // as if that is underway we might end up creating a new document post a DELETE was executed
        asyncSearchContext.acquireContextPermit(ActionListener.wrap(
                releasable -> {
                    boolean response = freeActiveContext(asyncSearchContext);
                    cancelTask(asyncSearchContext, "User triggered context deletion");
                    groupedDeletionListener.onResponse(response);
                    logger.debug("Deleting async search id [{}] from system index ", asyncSearchContext.getAsyncSearchId());
                    persistenceService.deleteResponse(asyncSearchContext.getAsyncSearchId(), null, groupedDeletionListener);
                    releasable.close();
                }, exception -> {
                    if (ExceptionsHelper.unwrapCause(exception) instanceof AlreadyClosedException == false) {
                        // this should ideally not happen. This would mean we couldn't acquire permits within the timeout
                        logger.warn(() -> new ParameterizedMessage("Failed to acquire permits for async search id [{}] for freeing context",
                                asyncSearchContext.getAsyncSearchId()), exception);
                    }
                    cancelTask(asyncSearchContext, "User triggered context deletion");
                    groupedDeletionListener.onResponse(false);
                    logger.debug("Deleting async search id [{}] from system index ", asyncSearchContext.getAsyncSearchId());
                    persistenceService.deleteResponse(asyncSearchContext.getAsyncSearchId(), null, groupedDeletionListener);
                }
        ), TimeValue.timeValueSeconds(5), "free context");
    }

    public boolean freeActiveContext(AsyncSearchActiveContext asyncSearchContext) {
        try {
            asyncSearchStateMachine.trigger(new SearchClosedEvent(asyncSearchContext));
            return true;
        } catch (AsyncSearchStateMachineClosedException ex) {
            logger.debug(() -> new ParameterizedMessage("Exception while freeing up active context"), ex);
            return false;
        }
    }

    /**
     * If an active context is found, a permit is acquired from
     * {@linkplain com.amazon.opendistroforelasticsearch.search.async.context.permits.AsyncSearchContextPermits} and on acquisition of
     * permit, a check is performed to see if response has been persisted in system index. If true, we update expiration in index. Else
     * we update expiration field in {@linkplain AsyncSearchActiveContext}.
     *
     * @param id                   async search id
     * @param keepAlive            the new keep alive duration
     * @param asyncSearchContextId async search context id
     * @param  user                current user
     * @param listener             listener to invoke after updating expiration.
     */
    public void updateKeepAliveAndGetContext(String id, TimeValue keepAlive, AsyncSearchContextId asyncSearchContextId, User user,
                                             ActionListener<AsyncSearchContext> listener) {
        validateKeepAlive(keepAlive);
        long requestedExpirationTime = currentTimeSupplier.getAsLong() + keepAlive.getMillis();
        // find an active context on this node if one exists
        Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        // for all other stages we don't really care much as those contexts are destined to be discarded
        if (asyncSearchContextOptional.isPresent()) {
            AsyncSearchActiveContext asyncSearchActiveContext = asyncSearchContextOptional.get();
            asyncSearchActiveContext.acquireContextPermit(ActionListener.wrap(
                    releasable -> {
                        // At this point it's possible that the response would have been persisted to system index
                        if (asyncSearchActiveContext.getAsyncSearchState() == AsyncSearchState.PERSISTED) {
                            logger.debug("Updating persistence store after state is PERSISTED async search id [{}] " +
                                    "for updating context", asyncSearchActiveContext.getAsyncSearchId());
                            persistenceService.updateExpirationTime(id, requestedExpirationTime, user, ActionListener.wrap(
                                    (actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(id, asyncSearchContextId,
                                            actionResponse, currentTimeSupplier, namedWriteableRegistry)), listener::onFailure));
                        } else {
                            if(isUserValid(user, asyncSearchActiveContext.getUser()) == false) {
                                listener.onFailure(new ElasticsearchSecurityException(
                                        "User doesn't have necessary roles to access the async search with id "+ id, RestStatus.FORBIDDEN));
                            }
                            else {
                                logger.debug("Updating persistence store: NO as state is NOT PERSISTED yet async search id [{}] " +
                                        "for updating context", asyncSearchActiveContext.getAsyncSearchId());
                                asyncSearchActiveContext.setExpirationTimeMillis(requestedExpirationTime);
                                listener.onResponse(asyncSearchActiveContext);
                            }
                        }
                        releasable.close();
                    },
                    exception -> {
                        if (ExceptionsHelper.unwrapCause(exception) instanceof AlreadyClosedException == false) {
                            // this should ideally not happen. This would mean we couldn't acquire permits within the timeout
                            logger.warn(() -> new ParameterizedMessage("Unexpected exception. Failed to acquire permits for " +
                                    "async search id [{}] for updating context", asyncSearchActiveContext.getAsyncSearchId()), exception);
                            listener.onFailure(exception);
                            return;
                        }
                        logger.debug("Updating persistence store after failing to acquire permits for async search id [{}] for updating " +
                                "context with expiration time [{}]", asyncSearchActiveContext.getAsyncSearchId(), requestedExpirationTime);
                        persistenceService.updateExpirationTime(id, requestedExpirationTime, user,
                                ActionListener.wrap((actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(
                                                id, asyncSearchContextId, actionResponse, currentTimeSupplier, namedWriteableRegistry)),
                                        listener::onFailure));
                        //TODO introduce request timeouts to make the permit wait transparent to the client
                    }), TimeValue.timeValueSeconds(5), "update keep alive");
        } else {
            // try update the doc on the index assuming there exists one.
            logger.debug("Updating persistence store after active context evicted for async search id [{}] " +
                    "for updating context", id);
            persistenceService.updateExpirationTime(id, requestedExpirationTime, user,
                    ActionListener.wrap((actionResponse) -> listener.onResponse(new AsyncSearchPersistenceContext(
                            id, asyncSearchContextId, actionResponse, currentTimeSupplier, namedWriteableRegistry)), listener::onFailure));
        }
    }


    public AsyncSearchStateMachine getStateMachine() {
        return asyncSearchStateMachine;
    }

    private AsyncSearchStateMachine initStateMachine() {
        AsyncSearchStateMachine stateMachine = new AsyncSearchStateMachine(
                EnumSet.allOf(AsyncSearchState.class), INIT);

        stateMachine.markTerminalStates(EnumSet.of(CLOSED));

        stateMachine.registerTransition(new AsyncSearchTransition<>(INIT, RUNNING,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).setTask(e.getSearchTask()),
                (contextId, listener) -> listener.onContextRunning(contextId), SearchStartedEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, SUCCEEDED,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchResponse(e.getSearchResponse()),
                (contextId, listener) -> listener.onContextCompleted(contextId), SearchSuccessfulEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, FAILED,
                (s, e) -> ((AsyncSearchActiveContext) e.asyncSearchContext()).processSearchFailure(e.getException()),
                (contextId, listener) -> listener.onContextFailed(contextId), SearchFailureEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(SUCCEEDED, PERSISTING,
                (s, e) -> asyncSearchPostProcessor.persistResponse((AsyncSearchActiveContext) e.asyncSearchContext(),
                        e.getAsyncSearchPersistenceModel()),
                (contextId, listener) -> {
                }, BeginPersistEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(FAILED, PERSISTING,
                (s, e) -> asyncSearchPostProcessor.persistResponse((AsyncSearchActiveContext) e.asyncSearchContext(),
                        e.getAsyncSearchPersistenceModel()),
                (contextId, listener) -> {
                }, BeginPersistEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(PERSISTING, PERSISTED,
                (s, e) -> asyncSearchActiveStore.freeContext(e.asyncSearchContext().getContextId()),
                (contextId, listener) -> listener.onContextPersisted(contextId), SearchResponsePersistedEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(PERSISTING, PERSIST_FAILED,
                (s, e) -> asyncSearchActiveStore.freeContext(e.asyncSearchContext().getContextId()),
                (contextId, listener) -> listener.onContextPersistFailed(contextId), SearchResponsePersistFailedEvent.class));

        stateMachine.registerTransition(new AsyncSearchTransition<>(RUNNING, CLOSED,
                (s, e) -> asyncSearchActiveStore.freeContext(e.asyncSearchContext().getContextId()),
                (contextId, listener) -> listener.onRunningContextClosed(contextId), SearchClosedEvent.class));

        for (AsyncSearchState state : EnumSet.of(PERSISTING, PERSISTED, PERSIST_FAILED, SUCCEEDED, FAILED, INIT)) {
            stateMachine.registerTransition(new AsyncSearchTransition<>(state, CLOSED,
                    (s, e) -> asyncSearchActiveStore.freeContext(e.asyncSearchContext().getContextId()),
                    (contextId, listener) -> listener.onContextClosed(contextId), SearchClosedEvent.class));
        }
        return stateMachine;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO listen to coordinator state failures and async search shards getting assigned
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        for (final AsyncSearchContext context : asyncSearchActiveStore.getAllContexts().values()) {
            freeActiveContext((AsyncSearchActiveContext) context);
        }
    }

    @Override
    protected void doClose() {
        doStop();
    }

    /**
     * @return Async Search stats accumulated on the current node
     */
    public AsyncSearchStats stats() {
        return statsListener.stats(clusterService.localNode());
    }


    private void validateRequest(SubmitAsyncSearchRequest request) {
        TimeValue keepAlive = request.getKeepAlive();
        validateKeepAlive(keepAlive);
        TimeValue waitForCompletionTimeout = request.getWaitForCompletionTimeout();
        validateWaitForCompletionTimeout(waitForCompletionTimeout);
    }

    private void validateWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        if (waitForCompletionTimeout.getMillis() > maxWaitForCompletion) {
            throw new IllegalArgumentException(
                    "Wait for completion timeout for async search (" + waitForCompletionTimeout.getMillis()
                            + ") is too large. It must be less than (" + TimeValue.timeValueMillis(maxWaitForCompletion)
                            + ").This limit can be set by changing the [" + MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey()
                            + "] cluster level setting.");
        }
    }

    private void validateKeepAlive(TimeValue keepAlive) {
        if (keepAlive.getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for async search (" + keepAlive.getMillis() + ") is too large. It must be less than (" +
                            TimeValue.timeValueMillis(maxKeepAlive) + ").This limit can be set by changing the ["
                            + MAX_KEEP_ALIVE_SETTING.getKey() + "] cluster level setting.");
        }
    }

}
