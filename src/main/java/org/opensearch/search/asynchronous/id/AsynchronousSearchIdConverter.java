/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.id;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.apache.lucene.store.ByteArrayDataInput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Base64;

public class AsynchronousSearchIdConverter {
    /**
     * Encodes the {@linkplain AsynchronousSearchId} in base64 encoding and returns an identifier for the submitted asynchronous search
     *
     * @param asynchronousSearchId The object to be encoded
     * @return The id which is used to access the submitted asynchronous search
     */
    public static String buildAsyncId(AsynchronousSearchId asynchronousSearchId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(asynchronousSearchId.getNode());
            out.writeString(String.valueOf(asynchronousSearchId.getTaskId()));
            out.writeString(asynchronousSearchId.getAsynchronousSearchContextId().getContextId());
            out.writeString(String.valueOf(asynchronousSearchId.getAsynchronousSearchContextId().getId()));
            return Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot build asynchronous search id", e);
        }
    }

    /**
     * Attempts to decode a base64 encoded string into an {@linkplain AsynchronousSearchId} which contains the details pertaining to
     * an asynchronous search being accessed.
     *
     * @param asynchronousSearchId The string to be decoded
     * @return The parsed {@linkplain AsynchronousSearchId}
     */
    public static AsynchronousSearchId parseAsyncId(String asynchronousSearchId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(asynchronousSearchId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            String node = in.readString();
            long taskId = Long.parseLong(in.readString());
            String contextId = in.readString();
            long id = Long.parseLong(in.readString());
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new AsynchronousSearchId(node, taskId, new AsynchronousSearchContextId(contextId, id));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse asynchronous search id", e);
        }
    }
}
