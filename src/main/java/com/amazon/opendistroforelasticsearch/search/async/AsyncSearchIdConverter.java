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
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Base64;

public class AsyncSearchIdConverter {
    /**
     * Encodes the {@linkplain AsyncSearchId} in base64 encoding and returns an identifier for the submitted async search
     *
     * @param asyncSearchId The object to be encoded
     * @return The id which is used to access the submitted async search
     */
    public static String buildAsyncId(AsyncSearchId asyncSearchId) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(asyncSearchId.getNode());
            out.writeLong(asyncSearchId.getTaskId());
            out.writeString(asyncSearchId.getAsyncSearchContextId().getContextId());
            out.writeLong(asyncSearchId.getAsyncSearchContextId().getId());
            return Base64.getUrlEncoder().withoutPadding().encodeToString(BytesReference.toBytes(out.bytes()));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot build async search id", e);
        }
    }

    /**
     * Attempts to decode a base64 encoded string into an {@linkplain AsyncSearchId} which contains the details pertaining to
     * an async search being accessed.
     *
     * @param asyncSearchId The string to be decoded
     * @return The parsed {@linkplain AsyncSearchId}
     */
    public static AsyncSearchId parseAsyncId(String asyncSearchId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(asyncSearchId);
            ByteArrayDataInput in = new ByteArrayDataInput(bytes);
            String node = in.readString();
            long taskId = in.readLong();
            String contextId = in.readString();
            long id = in.readLong();
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new AsyncSearchId(node, taskId, new AsyncSearchContextId(contextId, id));
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse async search id", e);
        }
    }
}
