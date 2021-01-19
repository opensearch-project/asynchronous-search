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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.persistence;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Base64;

/**
 * The model for persisting asynchronous search response to an index for retrieval after the search is complete
 */
public class AsynchronousSearchPersistenceModel {

    private final long expirationTimeMillis;
    private final long startTimeMillis;
    private final String response;
    private final String error;
    private final User user;

    public AsynchronousSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, String response, String error, User user) {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
        this.error = error;
        this.user = user;
    }

    public AsynchronousSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, SearchResponse response,
                                       Exception error, User user) throws IOException {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = serializeResponse(response);
        this.error = serializeError(error);
        this.user = user;
    }

    private String serializeResponse(SearchResponse response) throws IOException {
        if (response == null) {
            return null;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            response.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        }
    }

    /**
     * Serializes exception in string format
     *
     * @param error the exception
     * @return Serialized error
     * @throws IOException when serialization fails.
     * String type is required to store binary field in index
     */
    private String serializeError(Exception error) throws IOException {
        if (error == null) {
            return null;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Version.writeVersion(Version.CURRENT, out);
            out.writeException(error instanceof ElasticsearchException ? error : new ElasticsearchException(error));
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        }
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public String getError() {
        return error;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public User getUser() {
        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsynchronousSearchPersistenceModel other = (AsynchronousSearchPersistenceModel) o;
        return
                startTimeMillis == other.startTimeMillis && expirationTimeMillis == other.expirationTimeMillis
                        && ((response == null && other.response == null) ||
                        (response != null && other.response != null && response.equals(other.response)))
                        && ((error == null && other.error == null) ||
                        (error != null && other.error != null && error.equals(other.error)))
                        && ((user == null && other.user == null) ||
                        (user != null && other.user != null && user.equals(other.user)));

    }
}
