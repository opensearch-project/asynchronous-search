/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.request;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A request to fetch an asynchronous search response by id.
 */
public class GetAsynchronousSearchRequest extends AsynchronousSearchRoutingRequest<GetAsynchronousSearchRequest> {

    @Nullable
    private TimeValue waitForCompletionTimeout;

    @Nullable
    private TimeValue keepAlive;

    public GetAsynchronousSearchRequest(String id) {
        super(id);
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public void setWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }


    public GetAsynchronousSearchRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
        keepAlive = streamInput.readOptionalTimeValue();
        waitForCompletionTimeout = streamInput.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalTimeValue(keepAlive);
        out.writeOptionalTimeValue(waitForCompletionTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
