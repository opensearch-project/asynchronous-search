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

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Represents a persisted version of {@link AsyncSearchContext} through a backing {@link AsyncSearchPersistenceModel}
 */
public class AsyncSearchPersistenceContext extends AsyncSearchContext {

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceContext.class);

    private final String asyncSearchId;
    private final AsyncSearchPersistenceModel asyncSearchPersistenceModel;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public AsyncSearchPersistenceContext(String asyncSearchId, AsyncSearchContextId asyncSearchContextId,
                                         AsyncSearchPersistenceModel asyncSearchPersistenceModel,
                                         LongSupplier currentTimeSupplier,
                                         NamedWriteableRegistry namedWriteableRegistry) {
        super(asyncSearchContextId, currentTimeSupplier);
        Objects.requireNonNull(asyncSearchId);
        Objects.requireNonNull(asyncSearchContextId);
        Objects.requireNonNull(asyncSearchPersistenceModel);
        this.asyncSearchId = asyncSearchId;
        this.asyncSearchPersistenceModel = asyncSearchPersistenceModel;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public AsyncSearchPersistenceModel getAsyncSearchPersistenceModel() {
        return asyncSearchPersistenceModel;
    }

    @Override
    public String getAsyncSearchId() {
        return asyncSearchId;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public long getExpirationTimeMillis() {
        return asyncSearchPersistenceModel.getExpirationTimeMillis();
    }

    @Override
    public long getStartTimeMillis() {
        return asyncSearchPersistenceModel.getStartTimeMillis();
    }

    @Override
    public SearchResponse getSearchResponse() {
        if (asyncSearchPersistenceModel.getResponse() == null) {
            return null;
        } else {
            BytesReference bytesReference =
                    BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder().decode(
                            asyncSearchPersistenceModel.getResponse())));
            try (NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                    namedWriteableRegistry)) {
                return new SearchResponse(wrapperStreamInput);
            } catch (IOException e) {
                logger.error("Failed to parse search response " + asyncSearchPersistenceModel.getResponse(), e);
                return null;
            }
        }
    }

    @Override
    public Exception getSearchError() {
        if (asyncSearchPersistenceModel.getError() == null) {
            return null;
        }
        BytesReference bytesReference =
                BytesReference.fromByteBuffer(ByteBuffer.wrap(Base64.getUrlDecoder().decode(asyncSearchPersistenceModel.getError())));
        try (NamedWriteableAwareStreamInput wrapperStreamInput = new NamedWriteableAwareStreamInput(bytesReference.streamInput(),
                namedWriteableRegistry)) {
            return wrapperStreamInput.readException();
        } catch (IOException e) {
            logger.error("Failed to parse search error " + asyncSearchPersistenceModel.getError(), e);
            return null;
        }
    }

    @Override
    public AsyncSearchState getAsyncSearchState() {
        return AsyncSearchState.PERSISTED;
    }

    @Override
    public int hashCode() {
        return Objects.hash(asyncSearchId, asyncSearchPersistenceModel);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsyncSearchPersistenceContext asyncSearchPersistenceContext = (AsyncSearchPersistenceContext) o;
        return asyncSearchPersistenceContext.getAsyncSearchId()
                .equals(this.asyncSearchId) && asyncSearchPersistenceContext.getAsyncSearchPersistenceModel()
                .equals(this.asyncSearchPersistenceModel);
    }

}
