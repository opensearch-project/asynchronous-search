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

package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

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
            logger.error(() -> new ParameterizedMessage("Failed to create async search persistence model for async search [{}]",
                    asyncSearchContext.getAsyncSearchId()), e);
            return null;
        }
    }
}
