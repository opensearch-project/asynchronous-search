/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.request;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A request to delete an asynchronous search response based on its id
 */
public class DeleteAsynchronousSearchRequest extends AsynchronousSearchRoutingRequest<DeleteAsynchronousSearchRequest> {

    public DeleteAsynchronousSearchRequest(String id) {
        super(id);
    }

    public DeleteAsynchronousSearchRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
