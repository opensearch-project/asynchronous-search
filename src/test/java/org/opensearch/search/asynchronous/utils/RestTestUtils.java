/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.utils;

import org.opensearch.search.asynchronous.plugin.AsynchronousSearchPlugin;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Request;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

public class RestTestUtils {
    private static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    public static Request buildHttpRequest(SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest) throws IOException {

        return buildHttpRequest(submitAsynchronousSearchRequest, false);
    }

    public static Request buildHttpRequest(SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest
    , boolean isLegacy) throws IOException {

        SearchRequest searchRequest = submitAsynchronousSearchRequest.getSearchRequest();
    Request request =
        new Request(
            HttpPost.METHOD_NAME,
            /*trim first backslash*/
            endpoint(
                searchRequest.indices(),
                isLegacy
                    ? AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI.substring(1)
                    : AsynchronousSearchPlugin.BASE_URI.substring(1)));

        Params params = new Params();
        addSearchRequestParams(params, searchRequest);
        addSubmitAsynchronousSearchRequestParams(params, submitAsynchronousSearchRequest);

        if (searchRequest.source() != null) {
            request.setEntity(createEntity(searchRequest.source(), REQUEST_BODY_CONTENT_TYPE));
        }
        request.addParameters(params.asMap());
        return request;
    }




    public static Request buildHttpRequest(GetAsynchronousSearchRequest getAsynchronousSearchRequest) {
        return buildHttpRequest(getAsynchronousSearchRequest, false);
    }

    public static Request buildHttpRequest(GetAsynchronousSearchRequest getAsynchronousSearchRequest, boolean isLegacy) {
    Request request =
        new Request(
            HttpGet.METHOD_NAME,
            isLegacy
                ? AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI
                    + "/"
                    + getAsynchronousSearchRequest.getId()
                : AsynchronousSearchPlugin.BASE_URI + "/" + getAsynchronousSearchRequest.getId());
        Params params = new Params();
        addGetAsynchronousSearchRequestParams(params, getAsynchronousSearchRequest);
        request.addParameters(params.asMap());
        return request;
    }
    public static Request buildHttpRequest(DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest) {
        return buildHttpRequest(deleteAsynchronousSearchRequest, false);
    }


    public static Request buildHttpRequest(DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest, boolean isLegacy) {
    return new Request(
        HttpDelete.METHOD_NAME,
        isLegacy
            ? AsynchronousSearchPlugin.LEGACY_OPENDISTRO_BASE_URI
                + "/"
                + deleteAsynchronousSearchRequest.getId()
            : AsynchronousSearchPlugin.BASE_URI + "/" + deleteAsynchronousSearchRequest.getId());
  }

    private static void addGetAsynchronousSearchRequestParams(Params params, GetAsynchronousSearchRequest getAsynchronousSearchRequest) {
        params.withKeepAlive(getAsynchronousSearchRequest.getKeepAlive());
        params.withWaitForCompletionTimeout(getAsynchronousSearchRequest.getWaitForCompletionTimeout());
    }

    private static void addSubmitAsynchronousSearchRequestParams(Params params, SubmitAsynchronousSearchRequest
            submitAsynchronousSearchRequest) {
        params.withKeepAlive(submitAsynchronousSearchRequest.getKeepAlive());
        params.withWaitForCompletionTimeout(submitAsynchronousSearchRequest.getWaitForCompletionTimeout());
        params.withKeepOnCompletion(submitAsynchronousSearchRequest.getKeepOnCompletion());
    }

    static void addSearchRequestParams(Params params, SearchRequest searchRequest) {
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withRouting(searchRequest.routing());
        params.withPreference(searchRequest.preference());
        params.withIndicesOptions(searchRequest.indicesOptions());
        params.withSearchType(searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        params.putParam("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        if (searchRequest.getPreFilterShardSize() != null) {
            params.putParam("pre_filter_shard_size", Integer.toString(searchRequest.getPreFilterShardSize()));
        }
        params.withMaxConcurrentShardRequests(searchRequest.getMaxConcurrentShardRequests());
        if (searchRequest.requestCache() != null) {
            params.withRequestCache(searchRequest.requestCache());
        }
        if (searchRequest.allowPartialSearchResults() != null) {
            params.withAllowPartialResults(searchRequest.allowPartialSearchResults());
        }
        params.withBatchedReduceSize(searchRequest.getBatchedReduceSize());
        if (searchRequest.scroll() != null) {
            params.putParam("scroll", searchRequest.scroll().keepAlive());
        }
    }

    public static String endpoint(String[] indices, String endpoint) {
        return new EndpointBuilder().addCommaSeparatedPathParts(indices)
                .addPathPartAsIs(endpoint).build();
    }

    static class EndpointBuilder {

        private final StringJoiner joiner = new StringJoiner("/", "/", "");

        EndpointBuilder addPathPart(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(encodePart(part));
                }
            }
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(String[] parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addCommaSeparatedPathParts(List<String> parts) {
            addPathPart(String.join(",", parts));
            return this;
        }

        EndpointBuilder addPathPartAsIs(String... parts) {
            for (String part : parts) {
                if (Strings.hasLength(part)) {
                    joiner.add(part);
                }
            }
            return this;
        }

        String build() {
            return joiner.toString();
        }

        private static String encodePart(String pathPart) {
            try {
                //encode each part (e.g. index, type and id) separately before merging them into the path
                //we prepend "/" to the path part to make this path absolute, otherwise there can be issues with
                //paths that start with `-` or contain `:`
                //the authority must be an empty string and not null, else paths that being with slashes could have them
                //misinterpreted as part of the authority.
                URI uri = new URI(null, "", "/" + pathPart, null, null);
                //manually encode any slash that each part may contain
                return uri.getRawPath().substring(1).replaceAll("/", "%2F");
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Path part [" + pathPart + "] couldn't be encoded", e);
            }
        }
    }

    static class Params {
        private final Map<String, String> parameters = new HashMap<>();

        Params() {
        }

        Params putParam(String name, String value) {
            if (Strings.hasLength(value)) {
                parameters.put(name, value);
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Map<String, String> asMap() {
            return parameters;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
            if (fetchSourceContext != null) {
                if (fetchSourceContext.fetchSource() == false) {
                    putParam("_source", Boolean.FALSE.toString());
                }
                if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                    putParam("_source_includes", String.join(",", fetchSourceContext.includes()));
                }
                if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                    putParam("_source_excludes", String.join(",", fetchSourceContext.excludes()));
                }
            }
            return this;
        }

        Params withFields(String[] fields) {
            if (fields != null && fields.length > 0) {
                return putParam("fields", String.join(",", fields));
            }
            return this;
        }

        Params withMasterTimeout(TimeValue masterTimeout) {
            return putParam("master_timeout", masterTimeout);
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withSearchType(String searchType) {
            return putParam("search_type", searchType);
        }

        Params withMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
            return putParam("max_concurrent_shard_requests", Integer.toString(maxConcurrentShardRequests));
        }

        Params withBatchedReduceSize(int batchedReduceSize) {
            return putParam("batched_reduce_size", Integer.toString(batchedReduceSize));
        }

        Params withKeepAlive(TimeValue keepAlive) {
            return putParam("keep_alive", keepAlive);
        }

        Params withKeepOnCompletion(boolean keepOnCompletion) {
            return putParam("keep_on_completion", Boolean.toString(keepOnCompletion));
        }

        Params withRequestCache(boolean requestCache) {
            return putParam("request_cache", Boolean.toString(requestCache));
        }

        Params withAllowPartialResults(boolean allowPartialSearchResults) {
            return putParam("allow_partial_search_results", Boolean.toString(allowPartialSearchResults));
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withStoredFields(String[] storedFields) {
            if (storedFields != null && storedFields.length > 0) {
                return putParam("stored_fields", String.join(",", storedFields));
            }
            return this;
        }

        Params withTerminateAfter(int terminateAfter) {
            return putParam("terminate_after", String.valueOf(terminateAfter));
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIfSeqNo(long ifSeqNo) {
            if (ifSeqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                return putParam("if_seq_no", Long.toString(ifSeqNo));
            }
            return this;
        }

        Params withIfPrimaryTerm(long ifPrimaryTerm) {
            if (ifPrimaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                return putParam("if_primary_term", Long.toString(ifPrimaryTerm));
            }
            return this;
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount) {
            return withWaitForActiveShards(activeShardCount, ActiveShardCount.DEFAULT);
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount, ActiveShardCount defaultActiveShardCount) {
            if (activeShardCount != null && activeShardCount != defaultActiveShardCount) {
                return putParam("wait_for_active_shards", activeShardCount.toString().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            if (indicesOptions != null) {
                withIgnoreUnavailable(indicesOptions.ignoreUnavailable());
                putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
                String expandWildcards;
                if (indicesOptions.expandWildcardsOpen() == false && indicesOptions.expandWildcardsClosed() == false) {
                    expandWildcards = "none";
                } else {
                    StringJoiner joiner = new StringJoiner(",");
                    if (indicesOptions.expandWildcardsOpen()) {
                        joiner.add("open");
                    }
                    if (indicesOptions.expandWildcardsClosed()) {
                        joiner.add("closed");
                    }
                    expandWildcards = joiner.toString();
                }
                putParam("expand_wildcards", expandWildcards);
                putParam("ignore_throttled", Boolean.toString(indicesOptions.ignoreThrottled()));
            }
            return this;
        }

        Params withIgnoreUnavailable(boolean ignoreUnavailable) {
            // Always explicitly place the ignore_unavailable value.
            putParam("ignore_unavailable", Boolean.toString(ignoreUnavailable));
            return this;
        }

        Params withHuman(boolean human) {
            if (human) {
                putParam("human", Boolean.toString(human));
            }
            return this;
        }

        Params withLocal(boolean local) {
            if (local) {
                putParam("local", Boolean.toString(local));
            }
            return this;
        }

        Params withIncludeDefaults(boolean includeDefaults) {
            if (includeDefaults) {
                return putParam("include_defaults", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withPreserveExisting(boolean preserveExisting) {
            if (preserveExisting) {
                return putParam("preserve_existing", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withDetailed(boolean detailed) {
            if (detailed) {
                return putParam("detailed", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForCompletion(Boolean waitForCompletion) {
            return putParam("wait_for_completion", waitForCompletion.toString());
        }

        Params withWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
            return putParam("wait_for_completion_timeout", waitForCompletionTimeout);
        }

        Params withNodes(String[] nodes) {
            return withNodes(Arrays.asList(nodes));
        }

        Params withNodes(List<String> nodes) {
            if (nodes != null && nodes.size() > 0) {
                return putParam("nodes", String.join(",", nodes));
            }
            return this;
        }

        Params withActions(String[] actions) {
            return withActions(Arrays.asList(actions));
        }

        Params withActions(List<String> actions) {
            if (actions != null && actions.size() > 0) {
                return putParam("actions", String.join(",", actions));
            }
            return this;
        }

        Params withTaskId(org.opensearch.tasks.TaskId taskId) {
            if (taskId != null && taskId.isSet()) {
                return putParam("task_id", taskId.toString());
            }
            return this;
        }

        Params withParentTaskId(org.opensearch.tasks.TaskId parentTaskId) {
            if (parentTaskId != null && parentTaskId.isSet()) {
                return putParam("parent_task_id", parentTaskId.toString());
            }
            return this;
        }

        Params withWaitForStatus(ClusterHealthStatus status) {
            if (status != null) {
                return putParam("wait_for_status", status.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withWaitForNoRelocatingShards(boolean waitNoRelocatingShards) {
            if (waitNoRelocatingShards) {
                return putParam("wait_for_no_relocating_shards", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForNoInitializingShards(boolean waitNoInitShards) {
            if (waitNoInitShards) {
                return putParam("wait_for_no_initializing_shards", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withWaitForNodes(String waitForNodes) {
            return putParam("wait_for_nodes", waitForNodes);
        }

        Params withLevel(ClusterHealthRequest.Level level) {
            return putParam("level", level.name().toLowerCase(Locale.ROOT));
        }

        Params withWaitForEvents(Priority waitForEvents) {
            if (waitForEvents != null) {
                return putParam("wait_for_events", waitForEvents.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }
    }

    static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType) throws IOException {
        return createEntity(toXContent, xContentType, ToXContent.EMPTY_PARAMS);
    }

    static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType, ToXContent.Params toXContentParams)
            throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, xContentType, toXContentParams, false).toBytesRef();
        return new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
    }

    @SuppressForbidden(reason = "Only allowed place to convert a XContentType to a ContentType")
    public static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }
}
