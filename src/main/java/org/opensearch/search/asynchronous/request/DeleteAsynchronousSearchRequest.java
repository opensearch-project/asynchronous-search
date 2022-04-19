/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.request;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

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
