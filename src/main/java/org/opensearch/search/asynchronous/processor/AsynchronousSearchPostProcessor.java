/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.processor;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveContext;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchActiveStore;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchContextClosedException;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchStateMachine;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchStateMachineClosedException;
import org.opensearch.search.asynchronous.context.state.event.BeginPersistEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchFailureEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchResponsePersistFailedEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchResponsePersistedEvent;
import org.opensearch.search.asynchronous.context.state.event.SearchSuccessfulEvent;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;


/**
 * Performs the processing after a search completes.
 */
public class AsynchronousSearchPostProcessor {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchPostProcessor.class);

    private final AsynchronousSearchPersistenceService asynchronousSearchPersistenceService;
    private final AsynchronousSearchActiveStore asynchronousSearchActiveStore;
    private final AsynchronousSearchStateMachine asynchronousSearchStateMachine;
    private final Consumer<AsynchronousSearchActiveContext> freeActiveContextConsumer;
    private final ThreadPool threadPool;

    public AsynchronousSearchPostProcessor(AsynchronousSearchPersistenceService asynchronousSearchPersistenceService,
                                           AsynchronousSearchActiveStore asynchronousSearchActiveStore,
                                           AsynchronousSearchStateMachine stateMachine,
                                           Consumer<AsynchronousSearchActiveContext> freeActiveContextConsumer,
                                           ThreadPool threadPool, ClusterService clusterService) {
        this.asynchronousSearchActiveStore = asynchronousSearchActiveStore;
        this.asynchronousSearchPersistenceService = asynchronousSearchPersistenceService;
        this.asynchronousSearchStateMachine = stateMachine;
        this.freeActiveContextConsumer = freeActiveContextConsumer;
        this.threadPool = threadPool;
    }

    public AsynchronousSearchResponse processSearchFailure(Exception exception, AsynchronousSearchContextId asynchronousSearchContextId) {
        final Optional<AsynchronousSearchActiveContext> asynchronousSearchContextOptional = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        try {
            if (asynchronousSearchContextOptional.isPresent()) {
                AsynchronousSearchActiveContext asynchronousSearchContext = asynchronousSearchContextOptional.get();
                asynchronousSearchStateMachine.trigger(new SearchFailureEvent(asynchronousSearchContext, exception));
                handlePersist(asynchronousSearchContext);
                return asynchronousSearchContext.getAsynchronousSearchResponse();
            }
            // Best effort to return the response.
            return new AsynchronousSearchResponse(AsynchronousSearchState.FAILED, -1L, -1L, null,
                    ExceptionsHelper.convertToOpenSearchException(exception));
        } catch (AsynchronousSearchStateMachineClosedException ex) {
            // Best effort to return the response.
            return new AsynchronousSearchResponse(AsynchronousSearchState.FAILED, -1L, -1L, null,
                    ExceptionsHelper.convertToOpenSearchException(exception));
        }
    }

    public AsynchronousSearchResponse processSearchResponse(SearchResponse searchResponse,
                                                            AsynchronousSearchContextId asynchronousSearchContextId) {
        final Optional<AsynchronousSearchActiveContext> asynchronousSearchContextOptional = asynchronousSearchActiveStore
                .getContext(asynchronousSearchContextId);
        try {
            if (asynchronousSearchContextOptional.isPresent()) {
                AsynchronousSearchActiveContext asynchronousSearchContext = asynchronousSearchContextOptional.get();
                asynchronousSearchStateMachine.trigger(new SearchSuccessfulEvent(asynchronousSearchContext, searchResponse));
                handlePersist(asynchronousSearchContext);
                return asynchronousSearchContext.getAsynchronousSearchResponse();
            }
            // Best effort to return the response.
            return new AsynchronousSearchResponse(AsynchronousSearchState.SUCCEEDED, -1L, -1L, searchResponse, null);
        } catch (AsynchronousSearchStateMachineClosedException ex) {
            // Best effort to return the response.
            return new AsynchronousSearchResponse(AsynchronousSearchState.SUCCEEDED, -1L, -1L, searchResponse, null);
        }
    }

    public void persistResponse(AsynchronousSearchActiveContext asynchronousSearchContext,
                                AsynchronousSearchPersistenceModel persistenceModel) {
        // acquire all permits non-blocking
        asynchronousSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                    // check again after acquiring permit if the context has been deleted mean while
                    if (asynchronousSearchContext.shouldPersist() == false) {
                        logger.debug(
                                "Async search context [{}] has been closed while waiting to acquire permits for post processing",
                                asynchronousSearchContext.getAsynchronousSearchId());
                        releasable.close();
                        return;
                    }
                    logger.debug("Persisting response for asynchronous search id [{}]",
                            asynchronousSearchContext.getAsynchronousSearchId());
                    try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                        asynchronousSearchPersistenceService.storeResponse(asynchronousSearchContext.getAsynchronousSearchId(),
                                persistenceModel, ActionListener.runAfter(ActionListener.wrap(
                                        (indexResponse) -> {
                                            //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                            logger.debug("Successfully persisted response for asynchronous search id [{}]",
                                                    asynchronousSearchContext.getAsynchronousSearchId());
                                            try {
                                                asynchronousSearchStateMachine.trigger(new SearchResponsePersistedEvent(
                                                        asynchronousSearchContext));
                                            } catch (AsynchronousSearchStateMachineClosedException ex) {
                                                // this should never happen since we had checked after acquiring the all permits so a
                                                // concurrent delete is not expected here, however an external task cancellation
                                                // can cause this
                                                logger.warn("Unexpected state, possibly caused by external task cancellation," +
                                                                " context with id [{}] closed while triggering event [{}]",
                                                        asynchronousSearchContext.getAsynchronousSearchId(),
                                                        SearchResponsePersistedEvent.class.getName());
                                            } finally {
                                                freeActiveContextConsumer.accept(asynchronousSearchContext);
                                            }
                                        },

                                        (e) -> {
                                            try {
                                                asynchronousSearchStateMachine.trigger(new SearchResponsePersistFailedEvent(
                                                        asynchronousSearchContext));
                                            } catch (AsynchronousSearchStateMachineClosedException ex) {
                                                //this should never happen since we had checked after acquiring the all permits so a
                                                // concurrent delete is not expected here, however an external task cancellation
                                                // can cause this
                                                logger.warn("Unexpected state, possibly caused by external task cancellation," +
                                                                " context with id [{}] closed while triggering event [{}]",
                                                        asynchronousSearchContext.getAsynchronousSearchId(),
                                                        SearchResponsePersistFailedEvent.class.getName());
                                            } finally {
                                                freeActiveContextConsumer.accept(asynchronousSearchContext);
                                            }
                                            logger.error(() -> new ParameterizedMessage(
                                                    "Failed to persist final response for [{}] due to [{}]",
                                                    asynchronousSearchContext.getAsynchronousSearchId(), e));
                                        }
                                ), releasable::close));
                    }

                }, (e) -> {
                    // Failure to acquire context can happen either due to a TimeoutException or AsynchronousSearchAlreadyClosedException
                    // If we weren't able to acquire permits we clean up the context to release heap.
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Level level = cause instanceof AsynchronousSearchContextClosedException || cause instanceof TimeoutException
                            ? Level.DEBUG : Level.WARN;
                    logger.log(level, () -> new ParameterizedMessage("Exception  occured while acquiring the permit for " +
                            "asynchronousSearchContext [{}]", asynchronousSearchContext.getAsynchronousSearchId()), e);
                    freeActiveContextConsumer.accept(asynchronousSearchContext);
                }),
                TimeValue.timeValueSeconds(120), "persisting response");
    }

    private void handlePersist(AsynchronousSearchActiveContext asynchronousSearchContext) {
        if (asynchronousSearchContext.shouldPersist()) {
            try {
                asynchronousSearchStateMachine.trigger(new BeginPersistEvent(asynchronousSearchContext));
            } catch (AsynchronousSearchStateMachineClosedException e) {
                //very rare since we checked if the context is alive before firing this event
                //anyways clean it, it's idempotent
                freeActiveContextConsumer.accept(asynchronousSearchContext);
            }
        } else {
            freeActiveContextConsumer.accept(asynchronousSearchContext);
        }
    }
}
