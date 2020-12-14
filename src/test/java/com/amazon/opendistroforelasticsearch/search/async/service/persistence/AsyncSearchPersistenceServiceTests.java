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

package com.amazon.opendistroforelasticsearch.search.async.service.persistence;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AsyncSearchPersistenceServiceTests extends AsyncSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("persistenceServiceTests");
    }

    public void testCreateAndGetAndDeletee() throws IOException, InterruptedException {

        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsyncSearchId newAsyncSearchId = new AsyncSearchId(transportService.getLocalNode().getId(), 1, asyncSearchContextId);
        String id = AsyncSearchIdConverter.buildAsyncId(newAsyncSearchId);
        AsyncSearchResponse newAsyncSearchResponse = new AsyncSearchResponse(id,
                asyncSearchResponse.isRunning(),
                asyncSearchResponse.getStartTimeMillis(),
                asyncSearchResponse.getExpirationTimeMillis(),
                asyncSearchResponse.getSearchResponse(),
                asyncSearchResponse.getError());
        createDoc(persistenceService, newAsyncSearchResponse);

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(),
                ActionListener.wrap(r -> verifyPersistenceModel(new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                                asyncSearchResponse.getExpirationTimeMillis(), asyncSearchResponse.getSearchResponse()), r, getLatch),
                        e -> failure(getLatch, e)));
        getLatch.await();

        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteResponse(newAsyncSearchResponse.getId(),
                ActionListener.wrap(r -> assertBoolean(deleteLatch, r, true), e -> failure(deleteLatch, e)));
        deleteLatch.await();

        //assert failure
        CountDownLatch getLatch1 = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(),
                ActionListener.wrap((AsyncSearchPersistenceModel r) -> failure(getLatch1, new IllegalStateException("no response should " +
                        "have been found for async search " + id)), exception -> assertRnf(getLatch1, exception)))
        ;
        getLatch1.await();

    }

    public void testGetAndDeleteNonExistentId() throws InterruptedException, IOException, ExecutionException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        AsyncSearchId asyncSearchId = generateNewAsyncSearchId(transportService);
        AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse);
        CountDownLatch createLatch = new CountDownLatch(1);
        String id = AsyncSearchIdConverter.buildAsyncId(asyncSearchId);
        persistenceService.storeResponse(id, model1, ActionListener.wrap(
                r -> assertSuccessfulResponseCreation(id, r, createLatch), e -> failure(createLatch, e)));
        createLatch.await();
        CountDownLatch latch = new CountDownLatch(2);
        //assert failure
        persistenceService.getResponse("id", ActionListener.wrap((AsyncSearchPersistenceModel r) -> failure(latch,
                new IllegalStateException("no response should have been found for async search " + id)),
                exception -> assertRnf(latch, exception)));
        //assert failure
        persistenceService.deleteResponse("id", ActionListener.wrap((r) -> assertBoolean(latch, r, false), e -> failure(latch, e)));
        latch.await();

    }

    public void testCreateConcurrentDocsWhenIndexNotExists() throws InterruptedException, IOException, ExecutionException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        AsyncSearchId asyncSearchId1 = generateNewAsyncSearchId(transportService);
        AsyncSearchId asyncSearchId2 = generateNewAsyncSearchId(transportService);
        AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse);
        String id1 = AsyncSearchIdConverter.buildAsyncId(asyncSearchId1);

        AsyncSearchPersistenceModel model2 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse);
        String id2 = AsyncSearchIdConverter.buildAsyncId(asyncSearchId2);
        CountDownLatch createLatch = new CountDownLatch(2);
        threadPool.generic()
                .execute(() -> persistenceService.storeResponse(id1, model1, ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id1, r, createLatch), e -> failure(createLatch, e))));
        threadPool.generic()
                .execute(() -> persistenceService.storeResponse(id2, model2, ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id2, r, createLatch), e -> failure(createLatch, e))));
        createLatch.await();

        CountDownLatch getLatch1 = new CountDownLatch(1);
        persistenceService.getResponse(id1, ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                verifyPersistenceModel(model1, r, getLatch1), e -> failure(getLatch1, e)));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                verifyPersistenceModel(model2, r, getLatch2), e -> failure(getLatch2, e)));
        getLatch2.await();
    }

    public void testUpdateExpiration() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis();
        final AsyncSearchPersistenceModel newPersistenceModel = new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                newExpirationTime, asyncSearchResponse.getSearchResponse());
        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                newExpirationTime,
                ActionListener.wrap(persistenceModel -> {

                            verifyPersistenceModel(
                                    newPersistenceModel,
                                    persistenceModel,
                                    updateLatch);
                        },
                        e -> failure(updateLatch, e)));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), ActionListener.wrap(r -> {
            verifyPersistenceModel(newPersistenceModel, r, getLatch);
        }, e -> failure(getLatch, e)));
        getLatch.await();
    }

    public void testDeleteExpiredResponse() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.currentTimeMillis() + new TimeValue(100, TimeUnit.MILLISECONDS).getMillis();
        final AsyncSearchPersistenceModel newPersistenceModel = new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                newExpirationTime, asyncSearchResponse.getSearchResponse());
        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                newExpirationTime,
                ActionListener.wrap(persistenceModel -> {

                            verifyPersistenceModel(
                                    newPersistenceModel,
                                    persistenceModel,
                                    updateLatch);
                        },
                        e -> failure(updateLatch, e)));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), ActionListener.wrap(r -> {
            verifyPersistenceModel(newPersistenceModel, r, getLatch);
        }, e -> failure(getLatch, e)));
        getLatch.await();

        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteExpiredResponses(new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                assertTrue(acknowledgedResponse.isAcknowledged());
                deleteLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Received exception while deleting expired response");

            }
        }, System.currentTimeMillis());

    }

    private void assertRnf(CountDownLatch latch, Exception exception) {
        try {
            assertTrue("Expected : RNF. Actual : " + exception.getClass() + "with cause : " + exception.getCause(),
                    exception instanceof ResourceNotFoundException);
        } finally {
            latch.countDown();
        }
    }

    private void failure(CountDownLatch latch, Exception e) {
        latch.countDown();
        fail(e.getMessage());
    }

    private void createDoc(AsyncSearchPersistenceService persistenceService, AsyncSearchResponse asyncSearchResponse)
            throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.storeResponse(asyncSearchResponse.getId(),
                new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                        asyncSearchResponse.getExpirationTimeMillis(),
                        asyncSearchResponse.getSearchResponse()),
                ActionListener.wrap(r -> assertSuccessfulResponseCreation(asyncSearchResponse.getId(), r, latch), e -> failure(latch, e)));
        latch.await();
    }

    private AsyncSearchResponse getAsyncSearchResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        AsyncSearchResponse asyncSearchResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        return TestClientUtils.blockingGetAsyncSearchResponse(client(), new GetAsyncSearchRequest(asyncSearchResponse.getId()));
    }

    private AsyncSearchId generateNewAsyncSearchId(TransportService transportService) {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        return new AsyncSearchId(transportService.getLocalNode().getId(), randomInt(100), asyncSearchContextId);

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 1, TimeUnit.SECONDS);
    }

    @After
    public void deleteAsyncSearchIndex() throws InterruptedException {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        client().admin().indices().prepareDelete(INDEX).execute(ActionListener.wrap(r -> deleteLatch.countDown(), e -> {
            deleteLatch.countDown();
        }));
        deleteLatch.await();
    }

    private void assertBoolean(CountDownLatch latch, Boolean actual, Boolean expected) {
        try {
            assertEquals(actual, expected);
        } finally {
            latch.countDown();
        }
    }

    private void verifyPersistenceModel(
            AsyncSearchPersistenceModel expected, AsyncSearchPersistenceModel actual, CountDownLatch latch) {
        try {
            assertEquals(expected, actual);
        } finally {
            latch.countDown();

        }
    }

    private void assertSuccessfulResponseCreation(String id, IndexResponse r, CountDownLatch createLatch) {
        try {
            assertSame(r.getResult(), DocWriteResponse.Result.CREATED);
            assertEquals(r.getId(), id);
        } finally {
            createLatch.countDown();
        }
    }
}
