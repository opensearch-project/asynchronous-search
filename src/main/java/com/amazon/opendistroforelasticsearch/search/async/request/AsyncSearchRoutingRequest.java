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

package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A base request for routing async search request to the right node by id.
 */
public abstract class AsyncSearchRoutingRequest<Request extends AsyncSearchRoutingRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_CONNECTION_TIMEOUT = TimeValue.timeValueSeconds(10);

    /**
     * A timeout value in case the coordinator has not been discovered yet or disconnected.
     */
    protected TimeValue connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    private final String id;

    protected AsyncSearchRoutingRequest(String id) {
        this.id = id;
    }

    protected AsyncSearchRoutingRequest(StreamInput in) throws IOException {
        super(in);
        connectionTimeout = in.readOptionalTimeValue();
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalTimeValue(connectionTimeout);
        out.writeString(id);
    }

    @SuppressWarnings("unchecked")
    public final Request connectionTimeout(TimeValue timeout) {
        this.connectionTimeout = timeout;
        return (Request) this;
    }

    public String getId() {
        return this.id;
    }


    public final Request connectionTimeout(String timeout) {
        return connectionTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".connectionTimeout"));
    }

    public final TimeValue connectionTimeout() {
        return this.connectionTimeout;
    }
}
