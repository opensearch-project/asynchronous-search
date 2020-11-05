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

package com.amazon.opendistroforelasticsearch.search.async.response;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("id");
    private static final ParseField IS_RUNNING = new ParseField("is_running");
    private static final ParseField START_TIME_IN_MILLIS = new ParseField("start_time_in_millis");
    private static final ParseField EXPIRATION_TIME_IN_MILLIS = new ParseField("expiration_time_in_millis");
    private static final ParseField RESPONSE = new ParseField("response");
    private static final ParseField ERROR = new ParseField("error");


    private final String id;
    private final boolean isRunning;
    private final long startTimeMillis;
    private final long expirationTimeMillis;
    @Nullable
    private SearchResponse searchResponse;
    @Nullable
    private ElasticsearchException error;

    public boolean isRunning() {
        return isRunning;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public ElasticsearchException getError() {
        return error;
    }

    public AsyncSearchResponse(String id, boolean isRunning, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, ElasticsearchException error) {
        this.id = id;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.isRunning = in.readBoolean();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
        this.error = in.readBoolean() ? in.readException() :  null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(isRunning);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
        out.writeOptionalWriteable(searchResponse);
        if (error != null) {
            out.writeBoolean(true);
            out.writeException(error);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(IS_RUNNING.getPreferredName(), isRunning);
        builder.field(START_TIME_IN_MILLIS.getPreferredName(), startTimeMillis);
        builder.field(EXPIRATION_TIME_IN_MILLIS.getPreferredName(), expirationTimeMillis);
        if (searchResponse != null) {
            builder.field(RESPONSE.getPreferredName());
            searchResponse.toXContent(builder, params);
        }
        if (error != null) {
            builder.startObject(ERROR.getPreferredName());
            ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public String getId() {
        return id;
    }

    @Override
    public RestStatus status() {
        return searchResponse != null || error != null ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, isRunning, startTimeMillis, expirationTimeMillis, searchResponse, error);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsyncSearchResponse other = (AsyncSearchResponse) o;
        return  id == other.id &&
                isRunning== other.isRunning &&
                startTimeMillis == other.startTimeMillis &&
                expirationTimeMillis == other.expirationTimeMillis &&
                searchResponse == other.searchResponse &&
                error == other.error;
    }

    public static AsyncSearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static AsyncSearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
        String id = null;
        boolean isRunning = false;
        long startTimeMillis = -1;
        long expirationTimeMillis = -1;
        SearchResponse searchResponse = null;
        ElasticsearchException error = null;
        String currentFieldName = parser.currentName();

        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                if (RESPONSE.getPreferredName().equals(currentFieldName)) {
                    searchResponse = SearchResponse.fromXContent(parser);
                } else if (ERROR.getPreferredName().equals(currentFieldName)) {
                    error = ElasticsearchException.fromXContent(parser);
                } else {
                    continue;
                }
            } else if (token.isValue()) {
                if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    id = parser.text();
                } else if (START_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTimeMillis = parser.longValue();
                } else if (EXPIRATION_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    expirationTimeMillis = parser.longValue();
                } else if (IS_RUNNING.match(currentFieldName, parser.getDeprecationHandler())) {
                    isRunning = parser.booleanValue();
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new AsyncSearchResponse(id, isRunning, startTimeMillis, expirationTimeMillis, searchResponse, error);
    }

    //visible for testing
    public static AsyncSearchResponse empty(String id, SearchResponse searchResponse, ElasticsearchException exception) {
        return new AsyncSearchResponse(id, false, -1, -1, searchResponse, exception);
    }
}
