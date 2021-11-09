/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.request;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A base request for routing asynchronous search request to the right node by id.
 */
public abstract class AsynchronousSearchRoutingRequest<Request extends AsynchronousSearchRoutingRequest<Request>> extends ActionRequest {

    private final String id;

    protected AsynchronousSearchRoutingRequest(String id) {
        this.id = id;
    }

    protected AsynchronousSearchRoutingRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    public String getId() {
        return this.id;
    }

}
