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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A request to delete an async search response based on its id
 */
public class DeleteAsyncSearchRequest extends AsyncSearchRoutingRequest<DeleteAsyncSearchRequest> {

    public DeleteAsyncSearchRequest(String id) {
        super(id);
    }

    public DeleteAsyncSearchRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
