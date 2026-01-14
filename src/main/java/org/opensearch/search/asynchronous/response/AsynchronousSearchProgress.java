/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.response;

import org.opensearch.action.search.SearchShard;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

public class AsynchronousSearchProgress implements Writeable, ToXContentFragment {
    private static final ParseField SHARDS = new ParseField("shards");

    private final List<ShardProgress> shards;

    public AsynchronousSearchProgress(List<ShardProgress> shards) {
        this.shards = Collections.unmodifiableList(new ArrayList<>(shards));
    }

    public AsynchronousSearchProgress(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<ShardProgress> shards = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shards.add(new ShardProgress(in));
        }
        this.shards = Collections.unmodifiableList(shards);
    }

    public List<ShardProgress> getShards() {
        return shards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shards.size());
        for (ShardProgress shardProgress : shards) {
            shardProgress.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(SHARDS.getPreferredName());
        for (ShardProgress shardProgress : shards) {
            builder.startObject();
            shardProgress.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    public static AsynchronousSearchProgress fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<ShardProgress> shards = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (SHARDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                        shards.add(ShardProgress.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new AsynchronousSearchProgress(shards);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AsynchronousSearchProgress other = (AsynchronousSearchProgress) o;
        return shards.equals(other.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shards);
    }

    public static class ShardProgress implements Writeable, ToXContentFragment {
        private static final ParseField CLUSTER = new ParseField("cluster");
        private static final ParseField INDEX = new ParseField("index");
        private static final ParseField SHARD = new ParseField("shard");
        private static final ParseField MAX_DOC_ID_PROCESSED = new ParseField("max_doc_id_processed");
        private static final ParseField MAX_DOC = new ParseField("max_doc");

        private final String clusterAlias;
        private final String index;
        private final int shardId;
        private final long maxDocIdProcessed;
        private final long maxDoc;

        public ShardProgress(String clusterAlias, String index, int shardId, long maxDocIdProcessed, long maxDoc) {
            this.clusterAlias = clusterAlias;
            this.index = index;
            this.shardId = shardId;
            this.maxDocIdProcessed = maxDocIdProcessed;
            this.maxDoc = maxDoc;
        }

        public ShardProgress(StreamInput in) throws IOException {
            this.clusterAlias = in.readOptionalString();
            this.index = in.readString();
            this.shardId = in.readVInt();
            this.maxDocIdProcessed = in.readZLong();
            this.maxDoc = in.readZLong();
        }

        public static ShardProgress fromSearchShard(SearchShard shard, long maxDocIdProcessed, long maxDoc) {
            ShardId shardId = shard.getShardId();
            return new ShardProgress(shard.getClusterAlias(), shardId.getIndexName(), shardId.id(), maxDocIdProcessed, maxDoc);
        }

        public String getClusterAlias() {
            return clusterAlias;
        }

        public String getIndex() {
            return index;
        }

        public int getShardId() {
            return shardId;
        }

        public long getMaxDocIdProcessed() {
            return maxDocIdProcessed;
        }

        public long getMaxDoc() {
            return maxDoc;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(clusterAlias);
            out.writeString(index);
            out.writeVInt(shardId);
            out.writeZLong(maxDocIdProcessed);
            out.writeZLong(maxDoc);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (clusterAlias != null) {
                builder.field(CLUSTER.getPreferredName(), clusterAlias);
            }
            builder.field(INDEX.getPreferredName(), index);
            builder.field(SHARD.getPreferredName(), shardId);
            builder.field(MAX_DOC_ID_PROCESSED.getPreferredName(), maxDocIdProcessed);
            builder.field(MAX_DOC.getPreferredName(), maxDoc);
            return builder;
        }

        public static ShardProgress fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            String clusterAlias = null;
            String index = null;
            int shardId = -1;
            long maxDocIdProcessed = -1;
            long maxDoc = -1;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (CLUSTER.match(currentFieldName, parser.getDeprecationHandler())) {
                        clusterAlias = token == XContentParser.Token.VALUE_NULL ? null : parser.text();
                    } else if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        index = parser.text();
                    } else if (SHARD.match(currentFieldName, parser.getDeprecationHandler())) {
                        shardId = parser.intValue();
                    } else if (MAX_DOC_ID_PROCESSED.match(currentFieldName, parser.getDeprecationHandler())) {
                        maxDocIdProcessed = parser.longValue();
                    } else if (MAX_DOC.match(currentFieldName, parser.getDeprecationHandler())) {
                        maxDoc = parser.longValue();
                    } else {
                        parser.skipChildren();
                    }
                }
            }
            return new ShardProgress(clusterAlias, index, shardId, maxDocIdProcessed, maxDoc);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardProgress other = (ShardProgress) o;
            return shardId == other.shardId
                && maxDocIdProcessed == other.maxDocIdProcessed
                && maxDoc == other.maxDoc
                && Objects.equals(clusterAlias, other.clusterAlias)
                && Objects.equals(index, other.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterAlias, index, shardId, maxDocIdProcessed, maxDoc);
        }
    }
}
