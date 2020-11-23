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
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.ParserConstructor;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * The model for persisting async search response to an index for retrieval after the search is complete
 */
public class AsyncSearchPersistenceModel implements ToXContentObject {

    public static final String EXPIRATION_TIME_MILLIS = "expiration_time_millis";
    public static final String START_TIME_MILLIS = "start_time_millis";
    public static final String RESPONSE = "search_response";
    public static final String ERROR = "error";

    private final long expirationTimeMillis;
    private final long startTimeMillis;
    @Nullable
    private final BytesReference response;
    @Nullable
    private final BytesReference error;

    public static final InstantiatingObjectParser<AsyncSearchPersistenceModel, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<AsyncSearchPersistenceModel, Void>
                parser = InstantiatingObjectParser.builder("stored_response", true, AsyncSearchPersistenceModel.class);
        parser.declareLong(constructorArg(), new ParseField(START_TIME_MILLIS));
        parser.declareLong(constructorArg(), new ParseField(EXPIRATION_TIME_MILLIS));
        ObjectParserHelper<AsyncSearchPersistenceModel, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField(RESPONSE));
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField(ERROR));
        PARSER = parser.build();
    }

    @ParserConstructor
    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis,
                                       BytesReference response, BytesReference error) {
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.response = response;
        this.error = error;
    }

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
        this(startTimeMillis, expirationTimeMillis, null, toXContent(error));
    }

    /**
     * Construct a {@linkplain AsyncSearchPersistenceModel} for a search that completed succeeded with a response.
     *
     * @param startTimeMillis      start time in millis
     * @param expirationTimeMillis expiration time in millis
     * @param response             search response from the completed search request
     * @throws IOException when there is a serialization issue
     */
    public AsyncSearchPersistenceModel(long startTimeMillis, long expirationTimeMillis, ToXContent response) throws IOException {
        this(startTimeMillis, expirationTimeMillis,
                XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, true), null);
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public BytesReference getResponse() {
        return response;
    }

    public BytesReference getError() {
        return error;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(START_TIME_MILLIS, startTimeMillis);
        builder.field(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
        if (response != null) {
            XContentHelper.writeRawField(RESPONSE, response, Requests.INDEX_CONTENT_TYPE, builder, params);
        }
        if (error != null) {
            XContentHelper.writeRawField(ERROR, error, Requests.INDEX_CONTENT_TYPE, builder, params);
        }
        return builder;
    }

    private static BytesReference toXContent(Exception error) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }


    /**
     * Convert from XContent to a Map for easy processing.
     *
     * @return Will return an empty map if the task was finished with an error, hasn't yet finished, or didn't store its result.
     */
    public Map<String, Object> getResponseAsMap() {
        return response == null ? emptyMap() : convertToMap(response, false, Requests.INDEX_CONTENT_TYPE).v2();
    }

    /**
     * Convert from XContent to a Map for easy processing.
     *
     * @return Will return an empty map if the task didn't finish with an error, hasn't yet finished, or didn't store its result.
     */
    public Map<String, Object> getErrorAsMap() {
        if (error == null) {
            return emptyMap();
        }
        return convertToMap(error, false, Requests.INDEX_CONTENT_TYPE).v2();
    }

    @Override
    public String toString() {
        return Strings.toString(this, false, true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeMillis, expirationTimeMillis, response, error);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AsyncSearchPersistenceModel persistenceModel = (AsyncSearchPersistenceModel) o;
        return startTimeMillis == persistenceModel.startTimeMillis
                && expirationTimeMillis == persistenceModel.expirationTimeMillis
                && Objects.equals(getErrorAsMap(), persistenceModel.getErrorAsMap())
                && Objects.equals(getResponseAsMap(), persistenceModel.getResponseAsMap());
    }
}
