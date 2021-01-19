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

package com.amazon.opendistroforelasticsearch.search.asynchronous.service;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchTransition;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchDeletedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchStartedEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchContextEventListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.listener.AsynchronousSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.asynchronous.processor.AsynchronousSearchPostProcessor;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.AsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.stats.InternalAsynchronousSearchStats;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.AsynchronousSearchExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
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
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.CLOSED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.FAILED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.INIT;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.PERSISTING;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.PERSIST_FAILED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.PERSIST_SUCCEEDED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.RUNNING;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState.SUCCEEDED;
import static com.amazon.opendistroforelasticsearch.search.asynchronous.utils.UserAuthUtils.isUserValid;
import static org.elasticsearch.action.ActionListener.runAfter;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.unit.TimeValue.timeValueDays;
import static org.elasticsearch.common.unit.TimeValue.timeValueHours;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/***
 * Manages the lifetime of {@link AsynchronousSearchContext} for all the asynchronous searches running on the coordinator node.
 */

public class AsynchronousSearchService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchService.class);

    public static final Setting<TimeValue> MAX_KEEP_ALIVE_SETTING =
            Setting.positiveTimeSetting("opendistro_asynchronous_search.max_keep_alive", timeValueDays(5),
                    Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_SEARCH_RUNNING_TIME_SETTING =
            Setting.positiveTimeSetting("opendistro_asynchronous_search.max_search_running_time", timeValueHours(12),
                    Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
            "opendistro_asynchronous_search.max_wait_for_completion_timeout", timeValueMinutes(1), Setting.Property.NodeScope,
            Setting.Property.Dynamic);

    private volatile long maxKeepAlive;
    private volatile long maxWaitForCompletionTimeout;
    private volatile long maxSearchRunningTime;
    private final AtomicLong idGenerator = new AtomicLong();
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AsynchronousSearchPersistenceService persistenceService;
    private final AsynchronousSearchActiveStore asynchronousSearchActiveStore;
    private final AsynchronousSearchPostProcessor asynchronousSearchPostProcessor;
    private final LongSupplier currentTimeSupplier;
    private final AsynchronousSearchStateMachine asynchronousSearchStateMachine;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final AsynchronousSearchContextEventListener contextEventListener;

    public AsynchronousSearchService(AsynchronousSearchPersistenceService asynchronousSearchPersistenceService,
                                     AsynchronousSearchActiveStore asynchronousSearchActiveStore, Client client,
                                     ClusterService clusterService, ThreadPool threadPool,
                                     AsynchronousSearchContextEventListener contextEventListener,
                                     NamedWriteableRegistry namedWriteableRegistry) {
        this.contextEventListener = contextEventListener;
        this.client = client;
        Settings settings = clusterService.getSettings();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_KEEP_ALIVE_SETTING, this::setKeepAlive);
        setKeepAlive(MAX_KEEP_ALIVE_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING,
                this::setMaxWaitForCompletionTimeout);
        setMaxWaitForCompletionTimeout(MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_SEARCH_RUNNING_TIME_SETTING, this::setMaxSearchRunningTime);
        setMaxSearchRunningTime(MAX_SEARCH_RUNNING_TIME_SETTING.get(settings));
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.persistenceService = asynchronousSearchPersistenceService;
        this.currentTimeSupplier = System::currentTimeMillis;
        this.asynchronousSearchActiveStore = asynchronousSearchActiveStore;
        this.asynchronousSearchStateMachine = initStateMachine();
        this.asynchronousSearchPostProcessor = new AsynchronousSearchPostProcessor(persistenceService, asynchronousSearchActiveStore,
                asynchronousSearchStateMachine,
                this::freeActiveContext, threadPool);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    private void setMaxSearchRunningTime(TimeValue maxSearchRunningTime) {
        this.maxSearchRunningTime = maxSearchRunningTime.millis();
    }

    private void setMaxWaitForCompletionTimeout(TimeValue maxWaitForCompletionTimeout) {
        this.maxWaitForCompletionTimeout = maxWaitForCompletionTimeout.millis();
    }

    private void setKeepAlive(TimeValue maxKeepAlive) {
        this.maxKeepAlive = maxKeepAlive.millis();
    }

    /**
     * Creates a new active asynchronous search for a newly submitted asynchronous search.
     *
     * @param request                 the SubmitAsynchronousSearchRequest
     * @param relativeStartTimeMillis the relative start time of the search in millis
     * @param user                    current user
     * @param reduceContextBuilder    the reference for the reduceContextBuilder
     * @return the AsynchronousSearchContext for the submitted request
     */
    public AsynchronousSearchContext createAndStoreContext(SubmitAsynchronousSearchRequest request, long relativeStartTimeMillis,
                                                    Supplier<InternalAggregation.ReduceContextBuilder> reduceContextBuilder, User user) {
        validateRequest(request);
        AsynchronousSearchContextId asynchronousSearchContextId = new AsynchronousSearchContextId(UUIDs.base64UUID(),
                idGenerator.incrementAndGet());
        contextEventListener.onNewContext(asynchronousSearchContextId);
        AsynchronousSearchProgressListener progressActionListener = new AsynchronousSearchProgressListener(relativeStartTimeMillis,
                (response) -> asynchronousSearchPostProcessor.processSearchResponse(response, asynchronousSearchContextId),
                (e) -> asynchronousSearchPostProcessor.processSearchFailure(e, asynchronousSearchContextId),
                threadPool.executor(AsynchronousSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME),
                threadPool::relativeTimeInMillis,
                reduceContextBuilder);
        AsynchronousSearchActiveContext asynchronousSearchContext = new AsynchronousSearchActiveContext(asynchronousSearchContextId,
                clusterService.localNode().getId(),
                request.getKeepAlive(), request.getKeepOnCompletion(), threadPool, currentTimeSupplier, progressActionListener, user);
        asynchronousSearchActiveStore.putContext(asynchronousSearchContextId, asynchronousSearchContext,
                contextEventListener::onContextRejected);
        contextEventListener.onContextInitialized(asynchronousSearchContextId);
        return asynchronousSearchContext;
    }

    /**
     * Stores information of the {@linkplain SearchTask} in the asynchronous search and signals start of the the underlying
     * {@linkplain SearchAction}
     *
     * @param searchTask           The {@linkplain SearchTask} which stores information of the currently running {@linkplain SearchTask}
     * @param asynchronousSearchContextId the id of the active asyncsearch context
     */
    public void bootstrapSearch(SearchTask searchTask, AsynchronousSearchContextId asynchronousSearchContextId) {
        Optional<AsynchronousSearchActiveContext> asynchronousSearchContextOptional = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        if (asynchronousSearchContextOptional.isPresent()) {
            AsynchronousSearchActiveContext context = asynchronousSearchContextOptional.get();
            try {
                asynchronousSearchStateMachine.trigger(new SearchStartedEvent(context, searchTask));
            } catch (AsynchronousSearchStateMachineClosedException e) {
                throw new IllegalStateException(String.format(Locale.ROOT, "Unexpected! State machine already closed for " +
                        "context [%s] while triggering event [%s]", context.getAsynchronousSearchId(), SearchStartedEvent.class.getName()));
            }
        }
    }

    /**
     * Tries to find an {@linkplain AsynchronousSearchActiveContext}. If not found,
     * queries the {@linkplain AsynchronousSearchPersistenceService}  for a hit. If a response is found, it builds and returns an
     * {@linkplain AsynchronousSearchPersistenceContext}, else throws
     * {@linkplain ResourceNotFoundException}
     *
     * @param id                   The asynchronous search id
     * @param asynchronousSearchContextId the Async search context id
     * @param user                 current user
     * @param listener             to be invoked on finding an {@linkplain AsynchronousSearchContext}
     */
    public void findContext(String id, AsynchronousSearchContextId asynchronousSearchContextId, User user,
                            ActionListener<AsynchronousSearchContext> listener) {

        ActionListener<AsynchronousSearchContext> exceptionTranslationListener = getExceptionTranslationWrapper(id, listener);
        Optional<AsynchronousSearchActiveContext> optionalAsynchronousSearchActiveContext = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        // If context is CLOSED we can't acquire permits and hence can't update active context
        // so most likely a CLOSED context is stale
        if (optionalAsynchronousSearchActiveContext.isPresent() && optionalAsynchronousSearchActiveContext.get().isAlive()) {
            logger.debug("Active context is present for asynchronous search ID [{}]", id);
            AsynchronousSearchActiveContext asynchronousSearchActiveContext = optionalAsynchronousSearchActiveContext.get();
            if (isUserValid(user, asynchronousSearchActiveContext.getUser()) == false) {
                logger.debug("Invalid user requesting GET active context for asynchronous search id {}", id);
                exceptionTranslationListener.onFailure(new ElasticsearchSecurityException(
                        "User doesn't have necessary roles to access the asynchronous search with id " + id, RestStatus.FORBIDDEN));
            } else {
                exceptionTranslationListener.onResponse(asynchronousSearchActiveContext);
            }
        } else {
            logger.debug("Active context is not present for asynchronous search ID [{}]", id);
            persistenceService.getResponse(id, user, wrap(
                    (persistenceModel) ->
                            exceptionTranslationListener.onResponse(new AsynchronousSearchPersistenceContext(id,
                                    asynchronousSearchContextId, persistenceModel, currentTimeSupplier, namedWriteableRegistry)),
                    ex -> {
                        logger.debug(() -> new ParameterizedMessage("Context not found for ID  in the system index {}", id), ex);
                        exceptionTranslationListener.onFailure(ex);
                    }
            ));
        }
    }

    public Map<Long, AsynchronousSearchActiveContext> getAllActiveContexts() {
        return asynchronousSearchActiveStore.getAllContexts();
    }

    public Set<AsynchronousSearchContext> getContextsToReap() {
        Map<Long, AsynchronousSearchActiveContext> allContexts = asynchronousSearchActiveStore.getAllContexts();
        return Collections.unmodifiableSet(allContexts.values().stream()
                .filter(Objects::nonNull)
                .filter((c) -> EnumSet.of(CLOSED, PERSIST_FAILED).contains(c.getAsynchronousSearchState()) ||
                        isOverRunning(c) || c.isExpired())
                .collect(Collectors.toSet()));
    }


    /**
     * Attempts to find both an {@linkplain AsynchronousSearchActiveContext} and an {@linkplain AsynchronousSearchPersistenceContext}
     * and delete them. If at least one of the aforementioned objects are found and deleted successfully, the listener is invoked with
     * #true, else {@linkplain ResourceNotFoundException} is thrown.
     *
     * @param id                   asynchronous search id
     * @param asynchronousSearchContextId context id
     * @param user                 current user
     * @param listener             listener to invoke on deletion or failure to do so
     */
    public void freeContext(String id, AsynchronousSearchContextId asynchronousSearchContextId, User user,
                            ActionListener<Boolean> listener) {
        ActionListener<Boolean> exceptionTranslationWrapper = getExceptionTranslationWrapper(id, listener);
        Optional<AsynchronousSearchActiveContext> asynchronousSearchContextOptional = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        if (asynchronousSearchContextOptional.isPresent()) {
            logger.debug("Active context present for asynchronous search id [{}]", id);
            AsynchronousSearchActiveContext asynchronousSearchContext = asynchronousSearchContextOptional.get();
            if (isUserValid(user, asynchronousSearchContext.getUser())) {
                cancelAndFreeActiveAndPersistedContext(asynchronousSearchContext, exceptionTranslationWrapper, user);
            } else {
                exceptionTranslationWrapper.onFailure(new ElasticsearchSecurityException(
                        "User doesn't have necessary roles to access the asynchronous search with id " + id, RestStatus.FORBIDDEN));
            }
        } else {
            logger.debug("Active context NOT present for asynchronous search [{}]", id);
            //asynchronous search context didn't exist so obviously we didn't delete
            //deleted persisted context if one exists. If not the listener returns acknowledged as false
            //we don't need to acquire lock if the in-memory context doesn't exist. For persistence context we have a distributed view
            //with the last writer wins policy
            logger.debug("Deleting asynchronous search [{}] from system index ", id);
            persistenceService.deleteResponse(id, user, exceptionTranslationWrapper);
        }
    }

    private void cancelTask(AsynchronousSearchActiveContext asynchronousSearchContext, String reason, Runnable runnable) {
        if (asynchronousSearchContext.getTask() != null && asynchronousSearchContext.getTask().isCancelled() == false) {
            CancelTasksRequest cancelTasksRequest = new CancelTasksRequest()
                    .setTaskId(new TaskId(clusterService.localNode().getId(), asynchronousSearchContext.getTask().getId()))
                    .setReason(reason);
            client.admin().cluster().cancelTasks(cancelTasksRequest, runAfter(wrap(cancelTasksResponse ->
                            logger.debug("Successfully cancelled tasks [{}] with asynchronous search [{}] with response [{}]",
                                    asynchronousSearchContext.getTask(), asynchronousSearchContext.getAsynchronousSearchId(),
                                    cancelTasksResponse),
                    e -> logger.error(() -> new ParameterizedMessage("Failed to cancel task [{}] with asynchronous search [{}]" +
                            " with exception", asynchronousSearchContext.getTask(), asynchronousSearchContext.getAsynchronousSearchId()),
                            e)),
                    runnable));
        } else {
            runnable.run();
        }
    }

    // We are skipping user check in this while deleting from the persisted layer
    // as we have already checked for user in the present active context.
    private void cancelAndFreeActiveAndPersistedContext(AsynchronousSearchActiveContext asynchronousSearchContext,
                                                        ActionListener<Boolean> listener, User user) {
        // if there are no context found to be cleaned up we throw a ResourceNotFoundException
        AtomicReference<Releasable> releasableReference = new AtomicReference<>(() -> {});
        ActionListener<Boolean> releasableListener = runAfter(listener, releasableReference.get()::close);
        GroupedActionListener<Boolean> groupedDeletionListener = new GroupedActionListener<>(
                wrap((responses) -> {
                    if (responses.stream().anyMatch(r -> r)) {
                        logger.debug("Free context for asynchronous search [{}] successful ",
                                asynchronousSearchContext.getAsynchronousSearchId());
                        releasableListener.onResponse(true);
                    } else {
                        logger.debug("Freeing context, asynchronous search [{}] not found ",
                                asynchronousSearchContext.getAsynchronousSearchId());
                        releasableListener.onFailure(new ResourceNotFoundException(asynchronousSearchContext.getAsynchronousSearchId()));
                    }
                }, releasableListener::onFailure), 2);

        //We get a true or a ResourceNotFound from persistence layer. We want to translate it to either a true/false or any other exception
        //that should be surfaced up
        ActionListener<Boolean> translatedListener = wrap(
                groupedDeletionListener::onResponse, (ex) -> {
                    if (ex instanceof ResourceNotFoundException) {
                        groupedDeletionListener.onResponse(false);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("Translating exception, received for asynchronous search [{}]",
                                asynchronousSearchContext.getAsynchronousSearchId()), ex);
                        groupedDeletionListener.onFailure(ex);
                    }
                });
        String triggeredBy = user != null ? (" by user [" + user + "]") : "";
        String cancelTaskReason = "Delete asynchronous search [" + asynchronousSearchContext.getAsynchronousSearchId()
                + "] has been triggered" + triggeredBy + ". Attempting to cancel in-progress search task";
        //Intent of the lock here is to disallow ongoing migration to system index
        // as if that is underway we might end up creating a new document post a DELETE was executed
        asynchronousSearchContext.acquireContextPermitIfRequired(wrap(
                releasable -> {
                    releasableReference.set(releasable);
                    boolean response = freeActiveContext(asynchronousSearchContext);
                    if (asynchronousSearchContext.keepOnCompletion()) {
                        cancelTask(asynchronousSearchContext, cancelTaskReason, () -> groupedDeletionListener.onResponse(response));
                        logger.debug("Deleting asynchronous search id [{}] from system index ",
                                asynchronousSearchContext.getAsynchronousSearchId());
                        persistenceService.deleteResponse(asynchronousSearchContext.getAsynchronousSearchId(), user, translatedListener);
                    } else {
                        cancelTask(asynchronousSearchContext, cancelTaskReason, () -> {
                            if (response) {
                                releasableListener.onResponse(true);
                            } else {
                                releasableListener.onFailure(new ResourceNotFoundException(
                                        asynchronousSearchContext.getAsynchronousSearchId()));
                            }
                        });
                    }
                }, exception -> {
                    Throwable cause = ExceptionsHelper.unwrapCause(exception);
                    if (cause instanceof TimeoutException) {
                        // this should ideally not happen. This would mean we couldn't acquire permits within the timeout
                        logger.debug(() -> new ParameterizedMessage("Failed to acquire permits for " +
                                "asynchronous search id [{}] for updating context within timeout 5s",
                                asynchronousSearchContext.getAsynchronousSearchId()), exception);
                        listener.onFailure(new ElasticsearchTimeoutException(asynchronousSearchContext.getAsynchronousSearchId()));
                    } else {
                        // best effort clean up with acknowledged as false
                        if (asynchronousSearchContext.keepOnCompletion()) {
                            logger.debug(() -> new ParameterizedMessage("Failed to acquire permits for asynchronous search id " +
                                    "[{}] for freeing context", asynchronousSearchContext.getAsynchronousSearchId()), exception);
                            cancelTask(asynchronousSearchContext, cancelTaskReason, () -> groupedDeletionListener.onResponse(false));

                            logger.debug("Deleting asynchronous search id [{}] from system index ",
                                    asynchronousSearchContext.getAsynchronousSearchId());
                            persistenceService.deleteResponse(asynchronousSearchContext.getAsynchronousSearchId(),
                                    user, translatedListener);
                        } else {
                            logger.debug(() -> new ParameterizedMessage("Failed to acquire permits for asynchronous search id " +
                                    "[{}] for freeing context", asynchronousSearchContext.getAsynchronousSearchId()), exception);
                            cancelTask(asynchronousSearchContext, cancelTaskReason, () -> releasableListener.onResponse(false));
                        }
                    }
                }
        ), TimeValue.timeValueSeconds(5), "free context");
    }

    /**
     * Moves the context to CLOSED state. Must be invoked when the context needs to be completely removed from the
     * memory and move the state machine to a terminal state
     *
     * @param asynchronousSearchContext the active asynchronous search context
     * @return boolean indicating if the state machine moved the state to CLOSED
     */
    // TODO make this package private
    public boolean freeActiveContext(AsynchronousSearchActiveContext asynchronousSearchContext) {
        try {
            //TODO add asserts to ensure task is cancelled/completed/removed so that we don't leave orphan tasks
            asynchronousSearchStateMachine.trigger(new SearchDeletedEvent(asynchronousSearchContext));
            return true;
        } catch (AsynchronousSearchStateMachineClosedException ex) {
            logger.debug(() -> new ParameterizedMessage("Exception while freeing up active context"), ex);
            return false;
        }
    }

    /**
     * Executed when an on-going asynchronous search is cancelled
     *
     * @param asynchronousSearchContext the active asynchronous search context
     * @return boolean indicating if the state machine moved the state to DELETED
     */
    public boolean onCancelledFreeActiveContext(AsynchronousSearchActiveContext asynchronousSearchContext) {
        contextEventListener.onContextCancelled(asynchronousSearchContext.getContextId());
        return this.freeActiveContext(asynchronousSearchContext);
    }

    /**
     * If an active context is found, a permit is acquired from
     * {@linkplain com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits.AsynchronousSearchContextPermits}
     * and on acquisition of permit, a check is performed to see if response has been persisted in system index. If true, we update
     * expiration in index. Else we update expiration field in {@linkplain AsynchronousSearchActiveContext}.
     *
     * @param id                   asynchronous search id
     * @param keepAlive            the new keep alive duration
     * @param asynchronousSearchContextId asynchronous search context id
     * @param user                 current user
     * @param listener             listener to invoke after updating expiration.
     */
    public void updateKeepAliveAndGetContext(String id, TimeValue keepAlive, AsynchronousSearchContextId asynchronousSearchContextId,
                                             User user, ActionListener<AsynchronousSearchContext> listener) {
        ActionListener<AsynchronousSearchContext> exceptionTranslationWrapper = getExceptionTranslationWrapper(id, listener);
        validateKeepAlive(keepAlive);
        long requestedExpirationTime = currentTimeSupplier.getAsLong() + keepAlive.getMillis();
        // find an active context on this node if one exists
        Optional<AsynchronousSearchActiveContext> asynchronousSearchContextOptional = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        // for all other stages we don't really care much as those contexts are destined to be discarded
        if (asynchronousSearchContextOptional.isPresent()) {
            AsynchronousSearchActiveContext asynchronousSearchActiveContext = asynchronousSearchContextOptional.get();
            asynchronousSearchActiveContext.acquireContextPermitIfRequired(wrap(
                    releasable -> {
                        ActionListener<AsynchronousSearchContext> releasableActionListener = runAfter(exceptionTranslationWrapper,
                                releasable::close);
                        // At this point it's possible that the response would have been persisted to system index
                        if (asynchronousSearchActiveContext.isAlive() == false && asynchronousSearchActiveContext.keepOnCompletion()) {
                            logger.debug("Updating persistence store after state is PERSISTED asynchronous search id [{}] " +
                                    "for updating context", asynchronousSearchActiveContext.getAsynchronousSearchId());
                            persistenceService.updateExpirationTime(id, requestedExpirationTime, user, wrap(
                                    (actionResponse) ->
                                            releasableActionListener.onResponse(new AsynchronousSearchPersistenceContext(id,
                                                    asynchronousSearchContextId,
                                                    actionResponse, currentTimeSupplier, namedWriteableRegistry)),
                                    releasableActionListener::onFailure));
                        } else {
                            if (isUserValid(user, asynchronousSearchActiveContext.getUser())) {
                                logger.debug("Updating persistence store: NO as state is NOT PERSISTED yet asynchronous search id [{}] " +
                                        "for updating context", asynchronousSearchActiveContext.getAsynchronousSearchId());
                                asynchronousSearchActiveContext.setExpirationTimeMillis(requestedExpirationTime);
                                releasableActionListener.onResponse(asynchronousSearchActiveContext);
                            } else {
                                releasableActionListener.onFailure(
                                        new ElasticsearchSecurityException("User doesn't have necessary roles to access the " +
                                                "asynchronous search with id " + id, RestStatus.FORBIDDEN));
                            }
                        }
                    },
                    exception -> {
                        Throwable cause = ExceptionsHelper.unwrapCause(exception);
                        if (cause instanceof TimeoutException) {
                            // this should ideally not happen. This would mean we couldn't acquire permits within the timeout
                            logger.debug(() -> new ParameterizedMessage("Failed to acquire permits for " +
                                    "asynchronous search id [{}] for updating context within timeout 5s",
                                    asynchronousSearchActiveContext.getAsynchronousSearchId()), exception);
                            listener.onFailure(new ElasticsearchTimeoutException(id));
                        } else {
                            // best effort we try an update the doc if one exists
                            if (asynchronousSearchActiveContext.keepOnCompletion()) {
                                logger.debug(
                                        "Updating persistence store after failing to acquire permits for asynchronous search id [{}] for " +
                                                "updating context with expiration time [{}]", asynchronousSearchActiveContext
                                                .getAsynchronousSearchId(),
                                        requestedExpirationTime);
                                persistenceService.updateExpirationTime(id, requestedExpirationTime, user,
                                        wrap((actionResponse) -> exceptionTranslationWrapper.onResponse(
                                                new AsynchronousSearchPersistenceContext(id, asynchronousSearchContextId,
                                                        actionResponse, currentTimeSupplier, namedWriteableRegistry)),
                                                exceptionTranslationWrapper::onFailure));
                            } else {
                                exceptionTranslationWrapper.onFailure(new ResourceNotFoundException(
                                        asynchronousSearchActiveContext.getAsynchronousSearchId()));
                            }
                        }
                    }), TimeValue.timeValueSeconds(5), "update keep alive");
        } else {
            // try update the doc on the index assuming there exists one.
            logger.debug("Updating persistence store after active context evicted for asynchronous search id [{}] " +
                    "for updating context", id);
            persistenceService.updateExpirationTime(id, requestedExpirationTime, user,
                    wrap((actionResponse) -> exceptionTranslationWrapper.onResponse(new AsynchronousSearchPersistenceContext(
                                    id, asynchronousSearchContextId, actionResponse, currentTimeSupplier, namedWriteableRegistry)),
                            exceptionTranslationWrapper::onFailure));
        }
    }

    //TOD0 make this package private
    public AsynchronousSearchStateMachine getStateMachine() {
        return asynchronousSearchStateMachine;
    }

    private AsynchronousSearchStateMachine initStateMachine() {
        AsynchronousSearchStateMachine stateMachine = new AsynchronousSearchStateMachine(
                EnumSet.allOf(AsynchronousSearchState.class), INIT, contextEventListener);

        stateMachine.markTerminalStates(EnumSet.of(CLOSED));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(INIT, RUNNING,
                (s, e) -> ((AsynchronousSearchActiveContext) e.asynchronousSearchContext()).setTask(e.getSearchTask()),
                (contextId, listener) -> listener.onContextRunning(contextId), SearchStartedEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(RUNNING, SUCCEEDED,
                (s, e) -> ((AsynchronousSearchActiveContext) e.asynchronousSearchContext()).processSearchResponse(e.getSearchResponse()),
                (contextId, listener) -> listener.onContextCompleted(contextId), SearchSuccessfulEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(RUNNING, FAILED,
                (s, e) -> ((AsynchronousSearchActiveContext) e.asynchronousSearchContext()).processSearchFailure(e.getException()),
                (contextId, listener) -> listener.onContextFailed(contextId), SearchFailureEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(SUCCEEDED, PERSISTING,
                (s, e) -> asynchronousSearchPostProcessor.persistResponse((AsynchronousSearchActiveContext) e.asynchronousSearchContext(),
                        e.getAsynchronousSearchPersistenceModel()),
                (contextId, listener) -> {}, BeginPersistEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(FAILED, PERSISTING,
                (s, e) -> asynchronousSearchPostProcessor.persistResponse((AsynchronousSearchActiveContext) e.asynchronousSearchContext(),
                        e.getAsynchronousSearchPersistenceModel()),
                (contextId, listener) -> {}, BeginPersistEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(PERSISTING, PERSIST_SUCCEEDED,
                (s, e) -> {},
                (contextId, listener) -> listener.onContextPersisted(contextId), SearchResponsePersistedEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(PERSISTING, PERSIST_FAILED,
                (s, e) -> {},
                (contextId, listener) -> listener.onContextPersistFailed(contextId), SearchResponsePersistFailedEvent.class));

        stateMachine.registerTransition(new AsynchronousSearchTransition<>(RUNNING, CLOSED,
                (s, e) -> asynchronousSearchActiveStore.freeContext(e.asynchronousSearchContext().getContextId()),
                (contextId, listener) -> listener.onRunningContextDeleted(contextId), SearchDeletedEvent.class));

        for (AsynchronousSearchState state : EnumSet.of(PERSISTING, PERSIST_SUCCEEDED, PERSIST_FAILED, SUCCEEDED, FAILED, INIT)) {
            stateMachine.registerTransition(new AsynchronousSearchTransition<>(state, CLOSED,
                    (s, e) -> asynchronousSearchActiveStore.freeContext(e.asynchronousSearchContext().getContextId()),
                    (contextId, listener) -> listener.onContextDeleted(contextId), SearchDeletedEvent.class));
        }
        return stateMachine;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO listen to coordinator state failures and asynchronous search shards getting assigned
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
        for (final AsynchronousSearchContext context : asynchronousSearchActiveStore.getAllContexts().values()) {
            //TODO assert if tasks get cancelled on doStop
            freeActiveContext((AsynchronousSearchActiveContext) context);
        }
    }

    @Override
    protected void doClose() {
        doStop();
    }

    /**
     * @return Async Search stats accumulated on the current node
     */
    public AsynchronousSearchStats stats() {
        return ((InternalAsynchronousSearchStats) contextEventListener).stats(clusterService.localNode());
    }

    public long getMaxWaitForCompletionTimeout() {
        return maxWaitForCompletionTimeout;
    }

    private void validateRequest(SubmitAsynchronousSearchRequest request) {
        TimeValue keepAlive = request.getKeepAlive();
        validateKeepAlive(keepAlive);
        TimeValue waitForCompletionTimeout = request.getWaitForCompletionTimeout();
        validateWaitForCompletionTimeout(waitForCompletionTimeout);
    }

    private void validateWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        if (waitForCompletionTimeout.getMillis() > maxWaitForCompletionTimeout) {
            throw new IllegalArgumentException(
                    "Wait for completion timeout for asynchronous search (" + waitForCompletionTimeout.getMillis()
                            + ") is too large. It must be less than (" + TimeValue.timeValueMillis(maxWaitForCompletionTimeout)
                            + ").This limit can be set by changing the [" + MAX_WAIT_FOR_COMPLETION_TIMEOUT_SETTING.getKey()
                            + "] cluster level setting.");
        }
    }

    private void validateKeepAlive(TimeValue keepAlive) {
        if (keepAlive.getMillis() > maxKeepAlive) {
            throw new IllegalArgumentException(
                    "Keep alive for asynchronous search (" + keepAlive.getMillis() + ") is too large. It must be less than (" +
                            TimeValue.timeValueMillis(maxKeepAlive) + ").This limit can be set by changing the ["
                            + MAX_KEEP_ALIVE_SETTING.getKey() + "] cluster level setting.");
        }
    }

    /**
     * @param asynchronousSearchActiveContext the active context
     * @return Where the search has been running beyond the max search running time.
     */
    private boolean isOverRunning(AsynchronousSearchActiveContext asynchronousSearchActiveContext) {
        return EnumSet.of(RUNNING, INIT).contains(asynchronousSearchActiveContext.getAsynchronousSearchState()) &&
                asynchronousSearchActiveContext.getStartTimeMillis() + maxSearchRunningTime < threadPool.absoluteTimeInMillis();
    }

    private <T> ActionListener<T> getExceptionTranslationWrapper(String id, ActionListener<T> listener) {
        return wrap(listener::onResponse, e -> listener.onFailure(translateException(id, e)));
    }

    private Exception translateException(String id, Exception e) {
        if (e instanceof ResourceNotFoundException || e instanceof ElasticsearchSecurityException) {
            logger.debug(() -> new ParameterizedMessage("Translating exception received from operation on {}", id), e);
            return AsynchronousSearchExceptionUtils.buildResourceNotFoundException(id);
        } else {
            return e;
        }
    }
}
