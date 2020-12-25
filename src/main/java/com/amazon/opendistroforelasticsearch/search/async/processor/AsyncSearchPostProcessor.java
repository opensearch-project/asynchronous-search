package com.amazon.opendistroforelasticsearch.search.async.processor;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveStore;
import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchContextClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachine;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchStateMachineClosedException;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.BeginPersistEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchFailureEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistFailedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchResponsePersistedEvent;
import com.amazon.opendistroforelasticsearch.search.async.context.state.event.SearchSuccessfulEvent;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;
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

    public AsyncSearchPostProcessor(AsyncSearchPersistenceService asyncSearchPersistenceService,
                                    AsyncSearchActiveStore asyncSearchActiveStore, AsyncSearchStateMachine stateMachine,
                                    Consumer<AsyncSearchActiveContext> freeActiveContextConsumer) {
        this.asyncSearchActiveStore = asyncSearchActiveStore;
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
        this.asyncSearchStateMachine = stateMachine;
        this.freeActiveContextConsumer = freeActiveContextConsumer;
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
                        logger.warn("Async search context [{}] has been closed while waiting to acquire permits for post processing",
                                asyncSearchContext.getAsyncSearchId());
                        releasable.close();
                        return;
                    }
                    logger.warn("Persisting response for async search id [{}]", asyncSearchContext.getAsyncSearchId());
                    asyncSearchPersistenceService.storeResponse(asyncSearchContext.getAsyncSearchId(),
                            persistenceModel, ActionListener.runAfter(ActionListener.wrap(
                                    (indexResponse) -> {
                                        //Mark any dangling reference as PERSISTED and cleaning it up from the IN_MEMORY context
                                        logger.warn("Successfully persisted response for async search id [{}]",
                                                asyncSearchContext.getAsyncSearchId());
                                        try {
                                            asyncSearchStateMachine.trigger(new SearchResponsePersistedEvent(asyncSearchContext));
                                        } catch (AsyncSearchStateMachineClosedException ex) {
                                            // this should never happen since we had checked after acquiring the all permits so a
                                            // concurrent delete is not expected here
                                            throw new IllegalStateException(String.format(Locale.ROOT, "Unexpected, context with id [%s] " +
                                                            "closed while triggering event [$s]", asyncSearchContext.getAsyncSearchId(),
                                                    SearchResponsePersistedEvent.class.getName()));
                                        }
                                    },

                                    (e) -> {
                                        try {
                                            asyncSearchStateMachine.trigger(new SearchResponsePersistFailedEvent(asyncSearchContext));
                                        } catch (AsyncSearchStateMachineClosedException ex) {
                                            //this should never happen since we had checked after acquiring the all permits so a
                                            // concurrent delete is not expected here
                                            throw new IllegalStateException(String.format(Locale.ROOT,"Unexpected, state machine for " +
                                                     "context id [%s] closed while triggering event",
                                                    asyncSearchContext.getAsyncSearchId(),
                                                    SearchResponsePersistFailedEvent.class.getName()));
                                        }
                                        logger.error("Failed to persist final response for [{}] due to [{}]",
                                                asyncSearchContext.getAsyncSearchId(), e);
                                    }
                            ), releasable::close));

                }, (e) -> {
                    // Failure to acquire context can happen either due to a TimeoutException or AsyncSearchAlreadyClosedException
                    // If we weren't able to acquire permits we clean up the context to release heap.
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    Level level = cause instanceof AsyncSearchContextClosedException || cause instanceof TimeoutException
                            ? Level.DEBUG : Level.WARN;
                    logger.log(level, () -> new ParameterizedMessage("Exception while acquiring the permit for " +
                            "asyncSearchContext [{}] due to ", asyncSearchContext), e);
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
