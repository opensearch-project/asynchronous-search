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

package com.amazon.opendistroforelasticsearch.search.async.context.active;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class AsyncSearchRejectedException extends ElasticsearchException {

    private final long limit;

    public AsyncSearchRejectedException(String message, int limit) {
        super(message);
        this.limit = limit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(limit);
    }

    public AsyncSearchRejectedException(StreamInput in) throws IOException {
        super(in);
        this.limit = in.readLong();
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("limit", limit);
    }
}

