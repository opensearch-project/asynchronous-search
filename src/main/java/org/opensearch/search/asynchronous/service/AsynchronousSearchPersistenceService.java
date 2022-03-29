/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.service;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static org.opensearch.search.asynchronous.utils.UserAuthUtils.isUserValid;
import static org.opensearch.search.asynchronous.utils.UserAuthUtils.parseUser;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;

/**
 * Service that stores completed asynchronous search responses as documents in index, fetches asynchronous search response by id,
 * updates expiration time i.e. keep-alive and deletes asynchronous search responses.
 */
public class AsynchronousSearchPersistenceService {

    public static final String EXPIRATION_TIME_MILLIS = "expiration_time_millis";
    public static final String START_TIME_MILLIS = "start_time_millis";
    public static final String RESPONSE = "response";
    public static final String ERROR = "error";
    public static final String USER = "user";

    private static final Logger logger = LogManager.getLogger(AsynchronousSearchPersistenceService.class);
    public static final String ASYNC_SEARCH_RESPONSE_INDEX = ".opendistro-asynchronous-search-response";
    /**
     * The backoff policy to use when saving a asynchronous search response fails. The total wait
     * time is 600000 milliseconds, ten minutes.
     */
    public static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 14);
    public static final String BACKEND_ROLES = "backend_roles";
    public static final String SETTING_INDEX_CODEC = "index.codec";
    public static final String BEST_COMPRESSION_CODEC = "best_compression";

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public AsynchronousSearchPersistenceService(Client client, ClusterService clusterService, ThreadPool threadPool) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }


    /**
     * Creates asynchronous search response as document in index. Creates index if necessary, before creating document. Retries response
     * creation on failure with exponential backoff
     *
     * @param id               the asynchronous search id which also is used as document id for index
     * @param persistenceModel the dto containing asynchronous search response fields
     * @param listener         actionListener to invoke with indexResponse
     */
    public void storeResponse(String id, AsynchronousSearchPersistenceModel persistenceModel, ActionListener<IndexResponse> listener) {
        if (indexExists()) {
            doStoreResult(id, persistenceModel, listener);
        } else {
            createIndexAndDoStoreResult(id, persistenceModel, listener);
        }
    }

    /**
     * Fetches and de-serializes the asynchronous search response from index.
     *
     * @param id       asynchronous search id
     * @param user     current user
     * @param listener invoked once get request completes. Throws ResourceNotFoundException if index doesn't exist.
     */
    @SuppressWarnings("unchecked")
    public void getResponse(String id, User user, ActionListener<AsynchronousSearchPersistenceModel> listener) {
        if (indexExists() == false) {
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        GetRequest request = new GetRequest(ASYNC_SEARCH_RESPONSE_INDEX, id);
        client.get(request, ActionListener.wrap(getResponse ->
                {
                    if (getResponse.isExists()) {
                        Map<String, Object> source = getResponse.getSource();
                        AsynchronousSearchPersistenceModel asynchronousSearchPersistenceModel = new AsynchronousSearchPersistenceModel(
                                (long) source.get(START_TIME_MILLIS),
                                (long) source.get(EXPIRATION_TIME_MILLIS),
                                source.containsKey(RESPONSE) ? (String) source.get(RESPONSE) : null,
                                source.containsKey(ERROR) ? (String) source.get(ERROR) : null,
                                parseUser((Map<String, Object>) source.get(USER)));
                        if (isUserValid(user, asynchronousSearchPersistenceModel.getUser())) {
                            listener.onResponse(asynchronousSearchPersistenceModel);
                        } else {
                            logger.debug("Invalid user requesting GET persisted context for asynchronous search [{}]", id);
                            listener.onFailure(new OpenSearchSecurityException(
                                    "User doesn't have necessary roles to access the asynchronous search [" + id + "]",
                                    RestStatus.FORBIDDEN));
                        }
                    } else {
                        listener.onFailure(new ResourceNotFoundException(id));
                    }
                },
                exception -> {
                    logger.error(() -> new ParameterizedMessage("Failed to get response for asynchronous search [{}]", id),
                            exception);
                    final Throwable cause = ExceptionsHelper.unwrapCause(exception);
                    listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
                }));
    }


    /**
     * This method should be safe to call even if there isn't a prior document that exists. If the doc was actually deleted, the listener
     * returns true
     *
     * @param id       asynchronous search id
     * @param user     current user
     * @param listener invoked once delete document request completes.
     */

    public void deleteResponse(String id, User user, ActionListener<Boolean> listener) {
        if (indexExists() == false) {
            logger.debug("Async search index [{}] doesn't exists", ASYNC_SEARCH_RESPONSE_INDEX);
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        Consumer<Exception> onFailure = e -> {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof DocumentMissingException) {
                logger.debug(() -> new ParameterizedMessage("Async search response doc already deleted {}", id), e);
                listener.onFailure(new ResourceNotFoundException(id));
            } else {
                logger.debug(() -> new ParameterizedMessage("Failed to delete asynchronous search for id {}", id), e);
                listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
            }
        };
        if (user == null) {
            client.delete(new DeleteRequest(ASYNC_SEARCH_RESPONSE_INDEX, id), ActionListener.wrap(deleteResponse -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                    logger.debug("Delete asynchronous search {} successful. Returned result {}", id, deleteResponse.getResult());
                    listener.onResponse(true);
                } else {
                    logger.debug("Delete asynchronous search {} unsuccessful. Returned result {}", id, deleteResponse.getResult());
                    listener.onFailure(new ResourceNotFoundException(id));
                }
            }, onFailure));
        } else {
            UpdateRequest updateRequest = new UpdateRequest(ASYNC_SEARCH_RESPONSE_INDEX, id);
            String scriptCode = "if (ctx._source.user == null || ctx._source.user.backend_roles == null || " +
                    "( params.backend_roles!=null && params.backend_roles.containsAll(ctx._source.user.backend_roles))) " +
                    "{ ctx.op = 'delete' } else { ctx.op = 'none' }";
            Map<String, Object> params = new HashMap<>();
            params.put("backend_roles", user.getBackendRoles());
            Script deleteConditionallyScript = new Script(ScriptType.INLINE, "painless", scriptCode, params);
            updateRequest.script(deleteConditionallyScript);
            client.update(updateRequest, ActionListener.wrap(deleteResponse -> {
                switch (deleteResponse.getResult()) {
                    case UPDATED:
                        listener.onFailure(new IllegalStateException("Document updated when requesting delete for asynchronous search id "
                                + id));
                        break;
                    case NOOP:
                        listener.onFailure(new OpenSearchSecurityException(
                                "User doesn't have necessary roles to access the asynchronous search with id " + id, RestStatus.FORBIDDEN));
                        break;
                    case NOT_FOUND:
                        listener.onFailure(new ResourceNotFoundException(id));
                        break;
                    case DELETED:
                        listener.onResponse(true);
                        break;
                }
            }, onFailure));
        }
    }

    /**
     * Updates the expiration time field in index
     *
     * @param id                   asynchronous search id
     * @param expirationTimeMillis the new expiration time
     * @param user                 current user
     * @param listener             listener invoked with the response on completion of update request
     */
    @SuppressWarnings("unchecked")
    public void updateExpirationTime(String id, long expirationTimeMillis,
                                     User user, ActionListener<AsynchronousSearchPersistenceModel> listener) {
        if (indexExists() == false) {
            listener.onFailure(new ResourceNotFoundException(id));
            return;
        }
        UpdateRequest updateRequest = new UpdateRequest(ASYNC_SEARCH_RESPONSE_INDEX, id);
        updateRequest.retryOnConflict(5);
        if (user == null) {
            Map<String, Object> source = new HashMap<>();
            source.put(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
            updateRequest.doc(source, XContentType.JSON);
        } else {
            String scriptCode = "if (ctx._source.user == null || ctx._source.user.backend_roles == null || " +
                    "(params.backend_roles != null && params.backend_roles.containsAll(ctx._source.user.backend_roles))) " +
                    "{ ctx._source.expiration_time_millis = params.expiration_time_millis } else { ctx.op = 'none' }";
            Map<String, Object> params = new HashMap<>();
            params.put(BACKEND_ROLES, user.getBackendRoles());
            params.put(EXPIRATION_TIME_MILLIS, expirationTimeMillis);
            Script conditionalUpdateScript = new Script(ScriptType.INLINE, "painless", scriptCode, params);
            updateRequest.script(conditionalUpdateScript);
        }
        updateRequest.fetchSource(FetchSourceContext.FETCH_SOURCE);
        client.update(updateRequest, ActionListener.wrap(updateResponse -> {
            switch (updateResponse.getResult()) {
                case NOOP:
                    if (user != null) {
                        listener.onFailure(new OpenSearchSecurityException(
                                "User doesn't have necessary roles to access the asynchronous search with id " + id, RestStatus.FORBIDDEN));
                    } else {
                        Map<String, Object> updatedSource = updateResponse.getGetResult().getSource();
                        listener.onResponse(new AsynchronousSearchPersistenceModel((long) updatedSource.get(START_TIME_MILLIS),
                                (long) updatedSource.get(EXPIRATION_TIME_MILLIS),
                                (String) updatedSource.get(RESPONSE), (String) updatedSource.get(ERROR),
                                parseUser((Map<String, Object>) updatedSource.get(USER))));
                    }
                    break;
                case UPDATED:
                    Map<String, Object> updatedSource = updateResponse.getGetResult().getSource();
                    listener.onResponse(new AsynchronousSearchPersistenceModel((long) updatedSource.get(START_TIME_MILLIS),
                            (long) updatedSource.get(EXPIRATION_TIME_MILLIS),
                            (String) updatedSource.get(RESPONSE), (String) updatedSource.get(ERROR),
                            parseUser((Map<String, Object>) updatedSource.get(USER))));
                    break;
                case NOT_FOUND:
                case DELETED:
                    logger.debug("Update Result [{}] for id [{}], expiration time requested, [{}]",
                            updateResponse.getResult(), id, expirationTimeMillis);
                    listener.onFailure(new ResourceNotFoundException(id));
                    break;
            }
        }, exception -> {
            final Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof DocumentMissingException) {
                listener.onFailure(new ResourceNotFoundException(id));
            } else {
                logger.error(() -> new ParameterizedMessage("Exception occurred updating expiration time for asynchronous search [{}]",
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
            client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(
                deleteResponse -> {
                    if ((deleteResponse.getBulkFailures() != null && deleteResponse.getBulkFailures().size() > 0) ||
                            (deleteResponse.getSearchFailures() != null && deleteResponse.getSearchFailures().size() > 0)) {
                        logger.error("Failed to delete expired asynchronous search responses with bulk failures[{}] / search " +
                                "failures [{}]", deleteResponse.getBulkFailures(), deleteResponse.getSearchFailures());
                        listener.onResponse(new AcknowledgedResponse(false));

                    } else {
                        logger.debug("Successfully deleted expired responses");
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                },
                (e) -> {
                    logger.error(() -> new ParameterizedMessage("Failed to delete expired response for expiration time {}",
                            expirationTimeInMillis), e);
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    listener.onFailure(cause instanceof Exception ? (Exception) cause :
                            new NotSerializableExceptionWrapper(cause));
                })
            );
        }
    }

    private void createIndexAndDoStoreResult(String id, AsynchronousSearchPersistenceModel persistenceModel,
                                             ActionListener<IndexResponse> listener) {
        client.admin().indices().prepareCreate(ASYNC_SEARCH_RESPONSE_INDEX).setMapping(mapping())
                .setSettings(indexSettings()).execute(ActionListener.wrap(createIndexResponse -> doStoreResult(id, persistenceModel,
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

    private void doStoreResult(String id, AsynchronousSearchPersistenceModel model, ActionListener<IndexResponse> listener) {
        Map<String, Object> source = new HashMap<>();
        source.put(RESPONSE, model.getResponse());
        source.put(ERROR, model.getError());
        source.put(EXPIRATION_TIME_MILLIS, model.getExpirationTimeMillis());
        source.put(START_TIME_MILLIS, model.getStartTimeMillis());
        source.put(USER, model.getUser());
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(ASYNC_SEARCH_RESPONSE_INDEX)
                .setId(id).setSource(source, XContentType.JSON);
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
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                if ((cause instanceof OpenSearchRejectedExecutionException) && backoff.hasNext()) {
                    TimeValue wait = backoff.next();
                    logger.warn(() -> new ParameterizedMessage("failed to store asynchronous search response [{}], retrying in [{}]",
                            indexRequestBuilder.request().id(), wait), e);
                    threadPool.schedule(() -> doStoreResult(backoff, indexRequestBuilder, listener), wait, ThreadPool.Names.SAME);
                } else {
                    logger.error(() -> new ParameterizedMessage("failed to store asynchronous search response [{}], not retrying",
                            indexRequestBuilder.request().id()), e);
                    listener.onFailure(e);
                }
            }
        });
    }

    private Settings indexSettings() {
        return Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 5)
                .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
                .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .put(SETTING_INDEX_CODEC, BEST_COMPRESSION_CODEC)
                .build();
    }

    private XContentBuilder mapping() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder
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
