package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class BeginPersistEvent extends AsyncSearchContextEvent {

    private static final Logger logger = LogManager.getLogger(BeginPersistEvent.class);

    public BeginPersistEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }

    public AsyncSearchPersistenceModel getAsyncSearchPersistenceModel() {
        try {
            return new AsyncSearchPersistenceModel(asyncSearchContext.getStartTimeMillis(), asyncSearchContext.getExpirationTimeMillis(),
                    asyncSearchContext.getSearchResponse(), asyncSearchContext.getSearchError(), asyncSearchContext.getUser());
        } catch (IOException e) {
            logger.error("Failed to create async search persistence model for context " + asyncSearchContext, e);
            return null;
        }
    }
}
