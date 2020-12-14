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

package com.amazon.opendistroforelasticsearch.search.async.context.persistence;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Base64;

/**
 * The model for persisting async search response to an index for retrieval after the search is complete
 */
public class AsyncSearchPersistenceModel {

    private final long expirationTimeMillis;
    private final long startTimeMillis;
    private final String response;
    private final String error;

    /**
     * Construct a {@linkplain AsyncSearchPersistenceModel} for a search that completed with an error.
     *
     * @param startTimeMillis      start time in millis
     * @param expirationTimeMillis expiration time in millis
     * @param error                error from the completed search request
     * @throws IOException when there is a serialization issue
     */
    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis,
                                       Exception error) throws IOException {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = null;
        this.error = serializeError(error);
    }

    /**
     * Construct a {@linkplain AsyncSearchPersistenceModel} for a search that completed succeeded with a response.
     *
     * @param startTimeMillis      start time in millis
     * @param expirationTimeMillis expiration time in millis
     * @param response             search response from the completed search request
     * @throws IOException when there is a serialization issue
     */
    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, SearchResponse response) throws IOException {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = serializeResponse(response);
        this.error = null;
    }

    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, String response, String error) {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
        this.error = error;
    }

    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, SearchResponse response,
                                       Exception error) throws IOException {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = serializeResponse(response);
        this.error = serializeError(error);

    }

    private String serializeResponse(SearchResponse response) throws IOException {
        if (response == null) {
            return null;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
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
     * @see <a href="https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/lang/Object.html#hashCode()">
     * String type is required to store binary field in index </a>} Error is wrapped with elastic search exception to avoid
     * NotSerializableExceptionWrapper during deserialization
     */
    private String serializeError(Exception error) throws IOException {
        if (error == null) {
            return null;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsyncSearchPersistenceModel other = (AsyncSearchPersistenceModel) o;
        return
                startTimeMillis == other.startTimeMillis && expirationTimeMillis == other.expirationTimeMillis
                        && ((response == null && other.response == null) ||
                        (response != null && other.response != null && response.equals(other.response)))
                        && ((error == null && other.error == null) ||
                        (error != null && other.error != null && error.equals(other.error)));

    }
}
