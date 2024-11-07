/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.state.event;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContext;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchContextEvent;
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
            return new AsynchronousSearchPersistenceModel(
                asynchronousSearchContext.getStartTimeMillis(),
                asynchronousSearchContext.getExpirationTimeMillis(),
                asynchronousSearchContext.getSearchResponse(),
                asynchronousSearchContext.getSearchError(),
                asynchronousSearchContext.getUser()
            );
        } catch (IOException e) {
            logger.error(
                () -> new ParameterizedMessage(
                    "Failed to create asynchronous search persistence model" + " for asynchronous search [{}]",
                    asynchronousSearchContext.getAsynchronousSearchId()
                ),
                e
            );
            return null;
        }
    }
}
