/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.request;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get Async Search stats
 */
public class AsynchronousSearchStatsRequest extends BaseNodesRequest<AsynchronousSearchStatsRequest> {

    /**
     * Empty constructor needed for AsynchronousSearchStatsTransportAction
     */
    public AsynchronousSearchStatsRequest() {
        super((String[]) null);
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public AsynchronousSearchStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public AsynchronousSearchStatsRequest(String... nodeIds) {
        super(nodeIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

}
