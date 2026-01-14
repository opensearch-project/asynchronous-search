/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.response;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AsynchronousSearchProgressTests extends OpenSearchTestCase {

    public void testXContentRoundTrip() throws IOException {
        AsynchronousSearchProgress progress = createProgress();
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        builder.startObject();
        progress.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        BytesReference bytes = BytesReference.bytes(builder);
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                bytes,
                XContentType.JSON
            )
        ) {
            parser.nextToken();
            AsynchronousSearchProgress parsed = AsynchronousSearchProgress.fromXContent(parser);
            assertEquals(progress, parsed);
        }
    }

    public void testWireRoundTrip() throws IOException {
        AsynchronousSearchProgress progress = createProgress();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            progress.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AsynchronousSearchProgress parsed = new AsynchronousSearchProgress(in);
                assertEquals(progress, parsed);
            }
        }
    }

    private AsynchronousSearchProgress createProgress() {
        List<AsynchronousSearchProgress.ShardProgress> shards = new ArrayList<>();
        long maxDoc0 = randomLongBetween(0, 1000);
        long maxDocIdProcessed0 = randomLongBetween(0, maxDoc0);
        shards.add(new AsynchronousSearchProgress.ShardProgress(null, "index-0", 0, maxDocIdProcessed0, maxDoc0));
        long maxDoc1 = randomLongBetween(0, 1000);
        long maxDocIdProcessed1 = randomLongBetween(0, maxDoc1);
        shards.add(new AsynchronousSearchProgress.ShardProgress("cluster-1", "index-1", 1, maxDocIdProcessed1, maxDoc1));
        return new AsynchronousSearchProgress(shards);
    }
}
