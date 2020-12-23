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

import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Service that stores completed async search responses as documents in index, fetches async search response by id, updates expiration
 * time i.e. keep-alive and deletes async search responses.
 */
public class AsyncSearchPersistenceService {

    public static final String EXPIRATION_TIME_MILLIS = "expiration_time_millis";
    public static final String START_TIME_MILLIS = "start_time_millis";
    public static final String RESPONSE = "response";
    public static final String ERROR = "error";

    private static final Logger logger = LogManager.getLogger(AsyncSearchPersistenceService.class);
    public static final String ASYNC_SEARCH_RESPONSE_INDEX = ".opendistro_asynchronous_search_response";
    private static final String MAPPING_TYPE = "_doc";
    /**
     * The backoff policy to use when saving a async search response fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    public static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public AsyncSearchPersistenceService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }


    /**
     * Creates async search response as document in index. Creates index if necessary, before creating document. Retries response
     * creation on failure with exponential backoff
     *
     * @param id               the async search id which also is used as document id for index
     * @param persistenceModel the dto containing async search response fields
     * @param listener         actionListener to invoke with indexResponse
     */
    public void storeResponse(String id, AsyncSearchPersistenceModel persistenceModel, ActionListener<IndexResponse> listener) {
        if (indexExists()) {
            doStoreResult(id, persistenceModel, listener);
        } else {
            createIndexAndDoStoreResult(id, persistenceModel, listener);
        }
    }

    /**
     * Fetches and de-serializes the async search response from index.
     *
     * @param id       async search id
     * @param listener invoked once get request completes. Throws ResourceNotFoundException if index doesn't exist.
     */
    public void getResponse(String id, ActionListener<AsyncSearchPersistenceModel> listener) {
        if (indexExists() == false) {
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        GetRequest request = new GetRequest(ASYNC_SEARCH_RESPONSE_INDEX, id);
        client.get(request, ActionListener.wrap(getResponse ->
                {
                    if (getResponse.isExists()) {
                        Map<String, Object> source = getResponse.getSource();
                        listener.onResponse(new AsyncSearchPersistenceModel((long) source.get(START_TIME_MILLIS),
                                (long) source.get(EXPIRATION_TIME_MILLIS),
                                source.containsKey(RESPONSE) ? (String) source.get(RESPONSE) : null,
                                source.containsKey(ERROR) ? (String) source.get(ERROR) : null));
                    } else {
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
                },
                exception -> {
                    logger.error(() -> new ParameterizedMessage("Failed to get response for async search id {}", id), exception);
                    final Throwable cause = ExceptionsHelper.unwrapCause(exception);
                    listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                }));
    }


    /**
     * This method should be safe to call even if there isn't a prior document that exists. If the doc was actually deleted, the listener
     * returns true
     *
     * @param id       async search id
     * @param listener invoked once delete document request completes.
     */

    public void deleteResponse(String id, ActionListener<Boolean> listener) {
        if (indexExists() == false) {
            logger.warn("Async search index [{}] doesn't exists", ASYNC_SEARCH_RESPONSE_INDEX);
            listener.onResponse(false);
            return;
        }

        client.delete(new DeleteRequest(ASYNC_SEARCH_RESPONSE_INDEX, id), ActionListener.wrap(deleteResponse -> {
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                logger.warn("Delete async search {} successful. Returned result {}", id, deleteResponse.getResult());
                listener.onResponse(true);
            } else {
                logger.warn("Delete async search {} unsuccessful. Returned result {}", id, deleteResponse.getResult());
                listener.onResponse(false);
            }
        }, e -> {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof DocumentMissingException) {
                logger.warn(() -> new ParameterizedMessage("Async search response doc already deleted {}", id), e);
                listener.onResponse(false);
            } else {
                logger.warn(() -> new ParameterizedMessage("Failed to delete async search for id {}", id), e);
                listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
            }
        }));
    }

    /**
     * Updates the expiration time field in index
     *
     * @param id                   async search id
     * @param expirationTimeMillis the new expiration time
     * @param listener             listener invoked with the response on completion of update request
     */
    public void updateExpirationTime(String id, long expirationTimeMillis, ActionListener<AsyncSearchPersistenceModel> listener) {
        if (indexExists() == false) {
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        Map<String, Object> source = new HashMap<>();
        source.put(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
        UpdateRequest updateRequest = new UpdateRequest(ASYNC_SEARCH_RESPONSE_INDEX, id);
        updateRequest.doc(source, XContentType.JSON);
        updateRequest.retryOnConflict(5);
        updateRequest.fetchSource(FetchSourceContext.FETCH_SOURCE);
        client.update(updateRequest, ActionListener.wrap(updateResponse -> {
            switch (updateResponse.getResult()) {
                case UPDATED:
                case NOOP:
                    Map<String, Object> source1 = updateResponse.getGetResult().getSource();
                    listener.onResponse(new AsyncSearchPersistenceModel((long) source1.get(START_TIME_MILLIS),
                            (long) source1.get(EXPIRATION_TIME_MILLIS),
                            (String) source1.get(RESPONSE), (String) source1.get(ERROR)));
                    break;
                case NOT_FOUND:
                case DELETED:
                    listener.onFailure(new ResourceNotFoundException(id));
                    break;
            }
        }, exception -> {
            final Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof DocumentMissingException) {
                listener.onFailure(new ResourceNotFoundException(id));
            } else {
                logger.debug(() -> new ParameterizedMessage("Exception occurred updating expiration time for id {}",
                        id), exception);
                listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
            }
        }));

    }

    /**
     * Deletes all responses past a given expiration time
     *
     * @param listener               invoked once delete by query request completes
     * @param expirationTimeInMillis the expiration time
     */
    public void deleteExpiredResponses(ActionListener<AcknowledgedResponse> listener, long expirationTimeInMillis) {
        if (indexExists() == false) {
            logger.debug("Async search index not yet created! Nothing to delete.");
            listener.onResponse(new AcknowledgedResponse(true));
        } else {
            DeleteByQueryRequest request = new DeleteByQueryRequest(ASYNC_SEARCH_RESPONSE_INDEX)
                    .setQuery(QueryBuilders.rangeQuery(EXPIRATION_TIME_MILLIS).lte(expirationTimeInMillis));
            client.execute(DeleteByQueryAction.INSTANCE, request,
                 ActionListener.wrap(
                         deleteResponse -> {
                             if ((deleteResponse.getBulkFailures() != null &&  deleteResponse.getBulkFailures().size() > 0 ) ||
                                     (deleteResponse.getSearchFailures() != null && deleteResponse.getSearchFailures().size() > 0)) {
                                 logger.error("Failed to delete expired responses with bulk failures[{}] / search failures [{}] as ",
                                         deleteResponse.getBulkFailures(), deleteResponse.getSearchFailures());
                                 listener.onResponse(new AcknowledgedResponse(false));

                             } else {
                                 logger.debug("Successfully deleted expired responses");
                                 listener.onResponse(new AcknowledgedResponse(true));
                             }
                         },
                        (e) -> {
                            logger.debug(() -> new ParameterizedMessage("Failed to delete expired response for expiration time {}",
                                    expirationTimeInMillis), e);
                            final Throwable cause = ExceptionsHelper.unwrapCause(e);
                            listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                        }));
        }
    }

    private void createIndexAndDoStoreResult(String id, AsyncSearchPersistenceModel persistenceModel,
                                             ActionListener<IndexResponse> listener) {
        client.admin().indices().prepareCreate(ASYNC_SEARCH_RESPONSE_INDEX).addMapping(MAPPING_TYPE, mapping()).
                setSettings(indexSettings()).execute(ActionListener.wrap(createIndexResponse -> doStoreResult(id, persistenceModel,
                listener), exception -> {
            if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                try {
                    doStoreResult(id, persistenceModel, listener);
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    listener.onFailure(inner);
                }
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    private void doStoreResult(String id, AsyncSearchPersistenceModel model, ActionListener<IndexResponse> listener) {
        Map<String, Object> source = new HashMap<>();
        source.put(RESPONSE, model.getResponse());
        source.put(ERROR, model.getError());
        source.put(EXPIRATION_TIME_MILLIS, model.getExpirationTimeMillis());
        source.put(START_TIME_MILLIS, model.getStartTimeMillis());
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(ASYNC_SEARCH_RESPONSE_INDEX, MAPPING_TYPE,
                id).setSource(source, XContentType.JSON);
        doStoreResult(STORE_BACKOFF_POLICY.iterator(), indexRequestBuilder, listener);
    }

    private void doStoreResult(Iterator<TimeValue> backoff, IndexRequestBuilder indexRequestBuilder,
                               ActionListener<IndexResponse> listener) {
        indexRequestBuilder.execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(indexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                if (((e instanceof EsRejectedExecutionException || e instanceof ClusterBlockException
                        || e instanceof NoShardAvailableActionException) == false) || backoff.hasNext() == false) {
                    logger.warn(() -> new ParameterizedMessage("failed to store async search response, not retrying"), e);
                    listener.onFailure(e);
                } else {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store async search response, retrying in [{}]", wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, indexRequestBuilder, listener), wait, ThreadPool.Names.SAME);
                }
            }
        });
    }

    private Settings indexSettings() {
        return Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 5)
                .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
                .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
                .build();
    }

    private XContentBuilder mapping() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject()
                    .startObject("properties")
                        .startObject(START_TIME_MILLIS)
                            .field("type", "date")
                            .field("format", "epoch_millis")
                        .endObject()
                        .startObject(EXPIRATION_TIME_MILLIS)
                            .field("type", "date")
                            .field("format", "epoch_millis")
                        .endObject()
                        .startObject(RESPONSE)
                            .field("type", "binary")
                        .endObject()
                        .startObject(ERROR)
                            .field("type", "binary")
                        .endObject()
                    .endObject()
                    .endObject();
            return builder;
        } catch (IOException e) {
            throw new IllegalArgumentException("Async search persistence mapping cannot be read correctly.", e);
        }
    }

    private boolean indexExists() {
        return clusterService.state().routingTable().hasIndex(ASYNC_SEARCH_RESPONSE_INDEX);
    }
}
