package com.amazon.opendistroforelasticsearch.search.async.context.state;

public class AsyncSearchStateMachineClosedException extends AsyncSearchStateMachineException {

    public AsyncSearchStateMachineClosedException(AsyncSearchState currentState, AsyncSearchContextEvent event) {
        super(currentState, event);
    }
}
