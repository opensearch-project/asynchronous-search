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

package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import org.elasticsearch.test.ESTestCase;

import java.util.UUID;

public class AsyncSearchIdTests extends ESTestCase {

    public void testAsyncSearchIdParsing() {
        String node = UUID.randomUUID().toString();
        long taskId = randomNonNegativeLong();
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(
                UUID.randomUUID().toString(), randomNonNegativeLong());

        AsyncSearchId original = new AsyncSearchId(node, taskId, asyncSearchContextId);

        //generate identifier to access the submitted async search
        String id = AsyncSearchIdConverter.buildAsyncId(original);

        //parse the AsyncSearchId object which will contain information regarding node running the search, the associated task and
        // context id.
        AsyncSearchId parsed = AsyncSearchIdConverter.parseAsyncId(id);

        assertEquals(original, parsed);
    }


    public void testAsyncSearchIdParsingFailure() {
        String id = UUID.randomUUID().toString();
        //a possible precursor of ResourceNotFoundException(id), when a GET "/_opendistro/_asynchronous_search/{id}" is made
        // with an illegal id
        expectThrows(IllegalArgumentException.class, () -> AsyncSearchIdConverter.parseAsyncId(id));
    }
}
