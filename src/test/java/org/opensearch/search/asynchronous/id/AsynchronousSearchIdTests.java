/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.id;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
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
