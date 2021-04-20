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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.persistence;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContext;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Represents a persisted version of {@link AsynchronousSearchContext} through a backing {@link AsynchronousSearchPersistenceModel}
 */
public class AsynchronousSearchPersistenceContext extends AsynchronousSearchContext {

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchPersistenceContext.class);

    private final String asynchronousSearchId;
    private final AsynchronousSearchPersistenceModel asynchronousSearchPersistenceModel;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public AsynchronousSearchPersistenceContext(String asynchronousSearchId, AsynchronousSearchContextId asynchronousSearchContextId,
                                         AsynchronousSearchPersistenceModel asynchronousSearchPersistenceModel,
                                         LongSupplier currentTimeSupplier,
                                         NamedWriteableRegistry namedWriteableRegistry) {
        super(asynchronousSearchContextId, currentTimeSupplier);
        Objects.requireNonNull(asynchronousSearchId);
        Objects.requireNonNull(asynchronousSearchContextId);
        Objects.requireNonNull(asynchronousSearchPersistenceModel);
        this.asynchronousSearchId = asynchronousSearchId;
        this.asynchronousSearchPersistenceModel = asynchronousSearchPersistenceModel;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public AsynchronousSearchPersistenceModel getAsynchronousSearchPersistenceModel() {
        return asynchronousSearchPersistenceModel;
    }

    @Override
    public String getAsynchronousSearchId() {
        return asynchronousSearchId;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public long getExpirationTimeMillis() {
        return asynchronousSearchPersistenceModel.getExpirationTimeMillis();
    }

    @Override
    public long getStartTimeMillis() {
        return asynchronousSearchPersistenceModel.getStartTimeMillis();
    }

    @Override
    public SearchResponse getSearchResponse() {
        if (asynchronousSearchPersistenceModel.getResponse() == null) {
            return null;
        } else {
            BytesReference bytesReference =
                    BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder().decode(
                            asynchronousSearchPersistenceModel.getResponse())));
            try (NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                    namedWriteableRegistry)) {
                wrapperStreamInput.setVersion(Version.readVersion(wrapperStreamInput));
                return new SearchResponse(wrapperStreamInput);
            } catch (IOException e) {
                logger.error(() -> new ParameterizedMessage("Failed to parse search response for asynchronous search [{}] Response : [{}] ",
                        asynchronousSearchId, asynchronousSearchPersistenceModel.getResponse()), e);
                return null;
            }
        }
    }

    @Override
    public Exception getSearchError() {
        if (asynchronousSearchPersistenceModel.getError() == null) {
            return null;
        }
        BytesReference bytesReference =
                BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder()
                        .decode(asynchronousSearchPersistenceModel.getError())));
        try (NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                namedWriteableRegistry)) {
            wrapperStreamInput.setVersion(Version.readVersion(wrapperStreamInput));
            return wrapperStreamInput.readException();
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to parse search error for asynchronous search [{}] Error : [{}] ",
                    asynchronousSearchId, asynchronousSearchPersistenceModel.getResponse()), e);
            return null;
        }
    }

    @Override
    public User getUser() {
        return asynchronousSearchPersistenceModel.getUser();
    }

    @Override
    public AsynchronousSearchState getAsynchronousSearchState() {
        return AsynchronousSearchState.STORE_RESIDENT;
    }

    @Override
    public int hashCode() {
        return Objects.hash(asynchronousSearchId, asynchronousSearchPersistenceModel);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsynchronousSearchPersistenceContext asynchronousSearchPersistenceContext = (AsynchronousSearchPersistenceContext) o;
        return asynchronousSearchPersistenceContext.getAsynchronousSearchId()
                .equals(this.asynchronousSearchId) && asynchronousSearchPersistenceContext.getAsynchronousSearchPersistenceModel()
                .equals(this.asynchronousSearchPersistenceModel);
    }

}
