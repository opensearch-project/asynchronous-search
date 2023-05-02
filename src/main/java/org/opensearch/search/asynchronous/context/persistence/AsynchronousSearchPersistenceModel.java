/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.context.persistence;

import org.opensearch.commons.authuser.User;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;

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
            out.writeVersion(Version.CURRENT);
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
            out.writeVersion(Version.CURRENT);
            out.writeException(error instanceof OpenSearchException ? error : new OpenSearchException(error));
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
