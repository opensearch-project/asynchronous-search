package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchClosedEvent extends AsyncSearchContextEvent {

    public SearchClosedEvent(AsyncSearchActiveContext asyncSearchActiveContext) {
        super(asyncSearchActiveContext);
    }
}
