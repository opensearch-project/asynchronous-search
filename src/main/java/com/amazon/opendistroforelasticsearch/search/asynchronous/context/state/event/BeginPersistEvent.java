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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.event;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.io.IOException;

public class BeginPersistEvent extends AsynchronousSearchContextEvent {

    private static final Logger logger = LogManager.getLogger(BeginPersistEvent.class);

    public BeginPersistEvent(AsynchronousSearchContext asynchronousSearchContext) {
        super(asynchronousSearchContext);
    }

    public AsynchronousSearchPersistenceModel getAsynchronousSearchPersistenceModel() {
        try {
            return new AsynchronousSearchPersistenceModel(asynchronousSearchContext.getStartTimeMillis(),
                    asynchronousSearchContext.getExpirationTimeMillis(), asynchronousSearchContext.getSearchResponse(),
                    asynchronousSearchContext.getSearchError(), asynchronousSearchContext.getUser());
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to create asynchronous search persistence model" +
                    " for asynchronous search [{}]", asynchronousSearchContext.getAsynchronousSearchId()), e);
            return null;
        }
    }
}
