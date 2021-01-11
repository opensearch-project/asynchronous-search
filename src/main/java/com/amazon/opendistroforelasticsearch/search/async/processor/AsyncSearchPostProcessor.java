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

package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchContextClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;


/**
 * Performs the processing after a search completes.
 */
public class AsyncSearchPostProcessor {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPostProcessor.class);

    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final AsyncSearchActiveStore asyncSearchActiveStore;
    private final AsyncSearchStateMachine asyncSearchStateMachine;
    private final Consumer<AsyncSearchActiveContext> freeActiveContextConsumer;
    private final ThreadPool threadPool;

    public AsyncSearchPostProcessor(AsyncSearchPersistenceService asyncSearchPersistenceService,
                                    AsyncSearchActiveStore asyncSearchActiveStore, AsyncSearchStateMachine stateMachine,
                                    Consumer<AsyncSearchActiveContext> freeActiveContextConsumer,
                                    ThreadPool threadPool) {
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.asyncSearchStateMachine = stateMachine;
        this.freeActiveContextConsumer = freeActiveContextConsumer;
        this.threadPool = threadPool;

    }

    public AsyncSearchResponse processSearchFailure(Exception exception, AsyncSearchContextId asyncSearchContextId) {
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        try {
            if (asyncSearchContextOptional.isPresent()) {
                AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
                asyncSearchStateMachine.trigger(new SearchFailureEvent(asyncSearchContext, exception));
                handlePersist(asyncSearchContext);
                return asyncSearchContext.getAsyncSearchResponse();
            }
            // Best effort to return the response.
            return new AsyncSearchResponse(AsyncSearchState.FAILED, -1L, -1L, null,
                    ExceptionsHelper.convertToElastic(exception));
        } catch (AsyncSearchStateMachineClosedException ex) {
            // Best effort to return the response.
            return new AsyncSearchResponse(AsyncSearchState.FAILED, -1L, -1L, null,
                    ExceptionsHelper.convertToElastic(exception));
        }
    }

    public AsyncSearchResponse processSearchResponse(SearchResponse searchResponse, AsyncSearchContextId asyncSearchContextId) {
        final Optional<AsyncSearchActiveContext> asyncSearchContextOptional = asyncSearchActiveStore.getContext(asyncSearchContextId);
        try {
            if (asyncSearchContextOptional.isPresent()) {
                AsyncSearchActiveContext asyncSearchContext = asyncSearchContextOptional.get();
                asyncSearchStateMachine.trigger(new SearchSuccessfulEvent(asyncSearchContext, searchResponse));
                handlePersist(asyncSearchContext);
                return asyncSearchContext.getAsyncSearchResponse();
            }
            // Best effort to return the response.
            return new AsyncSearchResponse(AsyncSearchState.SUCCEEDED, -1L, -1L, searchResponse, null);
        } catch (AsyncSearchStateMachineClosedException ex) {
            // Best effort to return the response.
            return new AsyncSearchResponse(AsyncSearchState.SUCCEEDED, -1L, -1L, searchResponse, null);
        }
    }

    public void persistResponse(AsyncSearchActiveContext asyncSearchContext, AsyncSearchPersistenceModel persistenceModel) {
        // acquire all permits non-blocking
        asyncSearchContext.acquireAllContextPermits(ActionListener.wrap(releasable -> {
                    // check again after acquiring permit if the context has been deleted mean while
                    if (asyncSearchContext.shouldPersist() == false) {
                        logger.debug(
                                "Async search context [{}] has been closed while waiting to acquire permits for post processing",
                                asyncSearchContext.getAsyncSearchId());
                        releasable.close();
                        return;
                    }
                    logger.debug("Persisting response for async search id [{}]", asyncSearchContext.getAsyncSearchId());
                    try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                        asyncSearchPersistenceService.storeResponse(asyncSearchContext.getAsyncSearchId(),
                                persistenceModel, ActionListener.runAfter(ActionListener.wrap(
                                        (indexResponse) -> {
                                            //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                            logger.debug("Successfully persisted response for async search id [{}]",
                                                    asyncSearchContext.getAsyncSearchId());
                                            try {
                                                asyncSearchStateMachine.trigger(new SearchResponsePersistedEvent(asyncSearchContext));
                                            } catch (AsyncSearchStateMachineClosedException ex) {
                                                // this should never happen since we had checked after acquiring the all permits so a
                                                // concurrent delete is not expected here, however an external task cancellation
                                                // can cause this
                                                logger.warn("Unexpected state, possibly caused by external task cancellation," +
                                                                " context with id [{}] closed while triggering event [{}]",
                                                        asyncSearchContext.getAsyncSearchId(),
                                                        SearchResponsePersistedEvent.class.getName());
                                            } finally {
                                                freeActiveContextConsumer.accept(asyncSearchContext);
                                            }
                                        },

                                        (e) -> {
                                            try {
                                                asyncSearchStateMachine.trigger(new SearchResponsePersistFailedEvent(asyncSearchContext));
                                            } catch (AsyncSearchStateMachineClosedException ex) {
                                                //this should never happen since we had checked after acquiring the all permits so a
                                                // concurrent delete is not expected here, however an external task cancellation
                                                // can cause this
                                                logger.warn("Unexpected state, possibly caused by external task cancellation," +
                                                                " context with id [{}] closed while triggering event [{}]",
                                                        asyncSearchContext.getAsyncSearchId(),
                                                        SearchResponsePersistFailedEvent.class.getName());
                                            } finally {
                                                freeActiveContextConsumer.accept(asyncSearchContext);
                                            }
                                            logger.error(() -> new ParameterizedMessage(
                                                    "Failed to persist final response for [{}] due to [{}]",
                                                    asyncSearchContext.getAsyncSearchId(), e));
                                        }
                                ), releasable::close));
                    }

                }, (e) -> {
                    // Failure to acquire context can happen either due to a TimeoutException or AsyncSearchAlreadyClosedException
                    // If we weren't able to acquire permits we clean up the context to release heap.
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Level level = cause instanceof AsyncSearchContextClosedException || cause instanceof TimeoutException
                            ? Level.DEBUG : Level.WARN;
                    logger.log(level, () -> new ParameterizedMessage("Exception  occured while acquiring the permit for " +
                            "asyncSearchContext [{}]", asyncSearchContext.getAsyncSearchId()), e);
                    freeActiveContextConsumer.accept(asyncSearchContext);
                }),
                TimeValue.timeValueSeconds(120), "persisting response");
    }

    private void handlePersist(AsyncSearchActiveContext asyncSearchContext) {
        if (asyncSearchContext.shouldPersist()) {
            try {
                asyncSearchStateMachine.trigger(new BeginPersistEvent(asyncSearchContext));
            } catch (AsyncSearchStateMachineClosedException e) {
                //very rare since we checked if the context is alive before firing this event
                //anyways clean it, it's idempotent
                freeActiveContextConsumer.accept(asyncSearchContext);
            }
        } else {
            freeActiveContextConsumer.accept(asyncSearchContext);
        }
    }
}
