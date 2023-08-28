/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.response;

import org.opensearch.BaseExceptionsHelper;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.OpenSearchException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Requests;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class AsynchronousSearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("id");
    private static final ParseField STATE = new ParseField("state");
    private static final ParseField START_TIME_IN_MILLIS = new ParseField("start_time_in_millis");
    private static final ParseField EXPIRATION_TIME_IN_MILLIS = new ParseField("expiration_time_in_millis");
    private static final ParseField RESPONSE = new ParseField("response");
    private static final ParseField ERROR = new ParseField("error");
    @Nullable
    //when the search is cancelled we don't have the id
    private final String id;
    private final AsynchronousSearchState state;
    private final long startTimeMillis;
    private final long expirationTimeMillis;
    @Nullable
    private SearchResponse searchResponse;
    @Nullable
    private OpenSearchException error;

    public AsynchronousSearchResponse(String id, AsynchronousSearchState state, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, Exception error) {
        this.id = id;
        this.state = state;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error == null ? null : ExceptionsHelper.convertToOpenSearchException(error);
    }

    public AsynchronousSearchResponse(String id, AsynchronousSearchState state, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, OpenSearchException error) {
        this.id = id;
        this.state = state;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
    }

    public AsynchronousSearchResponse(AsynchronousSearchState state, long startTimeMillis, long expirationTimeMillis,
                               SearchResponse searchResponse, OpenSearchException error) {
        this.state = state;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.searchResponse = searchResponse;
        this.error = error;
        this.id = null;
    }

    public AsynchronousSearchResponse(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.state = in.readEnum(AsynchronousSearchState.class);
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
        this.error = in.readBoolean() ? in.readException() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeEnum(state);
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
        if (id != null) {
            builder.field(ID.getPreferredName(), id);
        }
        builder.field(STATE.getPreferredName(), state);
        builder.field(START_TIME_IN_MILLIS.getPreferredName(), startTimeMillis);
        builder.field(EXPIRATION_TIME_IN_MILLIS.getPreferredName(), expirationTimeMillis);
        if (searchResponse != null) {
            builder.startObject(RESPONSE.getPreferredName());
            searchResponse.innerToXContent(builder, params);
            builder.endObject();
        }
        if (error != null) {
            builder.startObject(ERROR.getPreferredName());
            BaseExceptionsHelper.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public AsynchronousSearchState getState() {
        return state;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public OpenSearchException getError() {
        return error;
    }

    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    public String getId() {
        return id;
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }


    /**
     * {@linkplain SearchResponse} and {@linkplain OpenSearchException} don't override hashcode, hence cannot be included in
     * the hashcode calculation for {@linkplain AsynchronousSearchResponse}. Given that we are using these methods only in tests; on the
     * off-chance that the {@link #equals(Object)} ()} comparison fails and hashcode is equal for 2
     * {@linkplain AsynchronousSearchResponse} objects, we are wary of the @see
     * <a href="https://docs.oracle.com/en/java/javase/14/docs/api/java.base/java/lang/Object.html#hashCode()">
     * performance improvement on hash tables </a>} that we forgo.
     *
     * @return hashcode of {@linkplain AsynchronousSearchResponse}
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, state, startTimeMillis, expirationTimeMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsynchronousSearchResponse other = (AsynchronousSearchResponse) o;
        try {
            return ((id == null && other.id == null) || (id != null && id.equals(other.id))) &&
                    state.equals(other.state) &&
                    startTimeMillis == other.startTimeMillis &&
                    expirationTimeMillis == other.expirationTimeMillis
                    && Objects.equals(getErrorAsMap(error), getErrorAsMap(other.error))
                    && Objects.equals(getResponseAsMap(searchResponse), getResponseAsMap(other.searchResponse));
        } catch (IOException e) {
            return false;
        }
    }

    private Map<String, Object> getErrorAsMap(OpenSearchException exception) throws IOException {
        if (exception != null) {
            BytesReference error;
            try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
                builder.startObject();
                BaseExceptionsHelper.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, exception);
                builder.endObject();
                error = BytesReference.bytes(builder);
                return convertToMap(error, false, Requests.INDEX_CONTENT_TYPE).v2();
            }
        } else {
            return emptyMap();
        }
    }

    private Map<String, Object> getResponseAsMap(SearchResponse searchResponse) throws IOException {
        if (searchResponse != null) {
            BytesReference response = XContentHelper.toXContent(searchResponse, Requests.INDEX_CONTENT_TYPE, true);
            if (response == null) {
                return emptyMap();
            }
            return convertToMap(response, false, Requests.INDEX_CONTENT_TYPE).v2();
        } else {
            return null;
        }
    }

    public static AsynchronousSearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static AsynchronousSearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String id = null;
        AsynchronousSearchState status = null;
        long startTimeMillis = -1;
        long expirationTimeMillis = -1;
        SearchResponse searchResponse = null;
        OpenSearchException error = null;
        String currentFieldName = null;
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            currentFieldName = parser.currentName();
            if (RESPONSE.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_OBJECT) {
                    ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                    searchResponse = SearchResponse.innerFromXContent(parser);
                }
            } else if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                parser.nextToken();
                error = OpenSearchException.fromXContent(parser);

            } else if (token.isValue()) {
                if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    id = parser.text();
                } else if (START_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTimeMillis = parser.longValue();
                } else if (EXPIRATION_TIME_IN_MILLIS.match(currentFieldName, parser.getDeprecationHandler())) {
                    expirationTimeMillis = parser.longValue();
                } else if (STATE.match(currentFieldName, parser.getDeprecationHandler())) {
                    status = AsynchronousSearchState.valueOf(parser.text());
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new AsynchronousSearchResponse(id, status, startTimeMillis, expirationTimeMillis, searchResponse, error);
    }

    //visible for testing
    public static AsynchronousSearchResponse empty(String id, SearchResponse searchResponse, Exception exception) {
        return new AsynchronousSearchResponse(id, null, -1, -1, searchResponse, exception);
    }
}
