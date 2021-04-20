/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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

package com.amazon.opendistroforelasticsearch.search.asynchronous.id;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.common.UUIDs;
import org.opensearch.test.OpenSearchTestCase;

import java.util.UUID;

public class AsynchronousSearchIdTests extends OpenSearchTestCase {

    public void testAsynchronousSearchIdParsing() {
        String node = "node" + randomIntBetween(1, 50);
        long taskId = randomNonNegativeLong();
        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(
                UUIDs.base64UUID(), randomNonNegativeLong());

        AsynchronousSearchId original = new AsynchronousSearchId(node, taskId, asContextId);
        //generate identifier to access the submitted asynchronous search
        String id = AsynchronousSearchIdConverter.buildAsyncId(original);
        //parse the AsynchronousSearchId object which will contain information regarding node running the search, the associated task and
        //context id.
        AsynchronousSearchId parsed = AsynchronousSearchIdConverter.parseAsyncId(id);
        assertEquals(original, parsed);
        assertEquals(parsed.toString(), "[" + node + "][" + taskId + "][" + asContextId + "]");
    }


    public void testAsynchronousSearchIdParsingFailure() {
        String id = UUID.randomUUID().toString();
        expectThrows(IllegalArgumentException.class, () -> AsynchronousSearchIdConverter.parseAsyncId(id));
    }

}
