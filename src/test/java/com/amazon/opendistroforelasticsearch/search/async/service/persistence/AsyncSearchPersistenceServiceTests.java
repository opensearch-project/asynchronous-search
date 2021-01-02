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

import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.async.commons.AsyncSearchSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.context.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchState;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.id.AsyncSearchIdConverter;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.service.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class AsyncSearchPersistenceServiceTests extends AsyncSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("persistenceServiceTests");
    }

    public void testCreateAndGetAndDelete() throws IOException, InterruptedException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        AsyncSearchResponse asyncSearchResponse = submitAndGetPersistedAsyncSearchResponse();

        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsyncSearchId newAsyncSearchId = new AsyncSearchId(transportService.getLocalNode().getId(), 1, asyncSearchContextId);
        String id = AsyncSearchIdConverter.buildAsyncId(newAsyncSearchId);
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User user : Arrays.asList(user1, null)) {
            AsyncSearchResponse newAsyncSearchResponse = new AsyncSearchResponse(id,
                    AsyncSearchState.STORE_RESIDENT,
                    asyncSearchResponse.getStartTimeMillis(),
                    asyncSearchResponse.getExpirationTimeMillis(),
                    asyncSearchResponse.getSearchResponse(),
                    asyncSearchResponse.getError());
            createDoc(persistenceService, newAsyncSearchResponse, user);

            if (user != null) {
                CountDownLatch getLatch1 = new CountDownLatch(1);
                ActionListener<AsyncSearchPersistenceModel> getListener = ActionListener.wrap(
                        r -> fail("Expected exception. Got " + r), e -> assertTrue(e instanceof ElasticsearchSecurityException));
                persistenceService.getResponse(newAsyncSearchResponse.getId(), user2, new LatchedActionListener<>(getListener, getLatch1));
                getLatch1.await();
            }
            CountDownLatch getLatch2 = new CountDownLatch(1);
            persistenceService.getResponse(newAsyncSearchResponse.getId(), user, new LatchedActionListener<>(
                    ActionListener.wrap(r -> assertEquals(
                            new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                                    asyncSearchResponse.getExpirationTimeMillis(), asyncSearchResponse.getSearchResponse(),
                                    null, user), r),
                            e -> {
                                logger.error("Expected get result got ", e);
                                fail(e.getMessage());
                            }), getLatch2));
            getLatch2.await();
            if (user != null) {
                CountDownLatch deleteLatch1 = new CountDownLatch(1);
                User diffUser = TestClientUtils.randomUser();
                ActionListener<Boolean> deleteListener = ActionListener.wrap(
                        r -> fail("Expected exception on delete. Got acknowledgment" + r),
                        e -> assertTrue(e instanceof ElasticsearchSecurityException));
                persistenceService.deleteResponse(newAsyncSearchResponse.getId(), diffUser,
                        new LatchedActionListener<>(deleteListener, deleteLatch1));
                deleteLatch1.await();
            }
            CountDownLatch deleteLatch2 = new CountDownLatch(1);
            ActionListener<Boolean> deleteListener = ActionListener.wrap(Assert::assertTrue, e -> {
                logger.debug(() -> new ParameterizedMessage("Delete failed unexpectedly "), e);
                fail("delete failed.expected success");
            });
            persistenceService.deleteResponse(newAsyncSearchResponse.getId(), user,
                    new LatchedActionListener<>(deleteListener, deleteLatch2));
            deleteLatch2.await();

            //assert failure
            CountDownLatch getLatch3 = new CountDownLatch(2);
            ActionListener<AsyncSearchPersistenceModel> getListener = ActionListener.wrap((r) -> fail("Expected RNF, Got " + r),
                    exception -> assertTrue(exception instanceof ResourceNotFoundException));
            persistenceService.getResponse(newAsyncSearchResponse.getId(), null, new LatchedActionListener<>(getListener, getLatch3));
            persistenceService.getResponse(newAsyncSearchResponse.getId(), user2, new LatchedActionListener<>(getListener, getLatch3));
            getLatch3.await();
        }
    }

    public void testGetAndDeleteNonExistentId() throws InterruptedException, IOException, ExecutionException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User originalUser : Arrays.asList(user1, null)) {
            AsyncSearchId asyncSearchId = generateNewAsyncSearchId(transportService);
            AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                    System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse, null, originalUser);
            CountDownLatch createLatch = new CountDownLatch(1);
            String id = AsyncSearchIdConverter.buildAsyncId(asyncSearchId);
            persistenceService.storeResponse(id, model1, new LatchedActionListener<>(ActionListener.wrap(
                    r -> assertSuccessfulResponseCreation(id, r), e -> {
                        logger.debug("expect successful create, got", e);
                        fail("Expected successful create, got " + e.getMessage());
                    }), createLatch));
            createLatch.await();
            for (User currentuser : Arrays.asList(originalUser, user2)) {
                CountDownLatch latch = new CountDownLatch(2);
                //assert failure
                persistenceService.getResponse("id", currentuser, new LatchedActionListener<>(
                        ActionListener.wrap((AsyncSearchPersistenceModel r) -> fail("Excepted resource_not_found_exception, got " + r),
                                exception -> assertTrue("Expected resource_not_found expection, got " + exception.getClass().toString(),
                                        exception instanceof ResourceNotFoundException)), latch));
                //assert failure
                ActionListener<Boolean> wrap = ActionListener.wrap(
                        r -> fail("Expected resource_not_found expection on delete, got acknowledgement " + r),
                        ex -> assertTrue("Expected resource_not_found expection, got " + ex.getClass().toString(),
                                ex instanceof ResourceNotFoundException));
                persistenceService.deleteResponse("id", currentuser, new LatchedActionListener<>(wrap, latch));
                latch.await();
            }
        }

    }

    public void testCreateConcurrentDocsWhenIndexNotExists() throws InterruptedException, IOException, ExecutionException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        AsyncSearchId asyncSearchId1 = generateNewAsyncSearchId(transportService);
        AsyncSearchId asyncSearchId2 = generateNewAsyncSearchId(transportService);
        AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse, null, null);
        String id1 = AsyncSearchIdConverter.buildAsyncId(asyncSearchId1);

        AsyncSearchPersistenceModel model2 = new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse, null, null);
        String id2 = AsyncSearchIdConverter.buildAsyncId(asyncSearchId2);
        CountDownLatch createLatch = new CountDownLatch(2);
        threadPool.generic()
                .execute(() -> persistenceService.storeResponse(id1, model1, new LatchedActionListener<>(ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id1, r), e -> {
                            logger.debug("expect successful create, got", e);
                            fail("Expected successful create, got " + e.getMessage());
                        }), createLatch)));
        threadPool.generic()
                .execute(() -> persistenceService.storeResponse(id2, model2, new LatchedActionListener<>(ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id2, r), e -> {
                            logger.debug("expect successful create, got", e);
                            fail("Expected successful create, got " + e.getMessage());
                        }), createLatch)));
        createLatch.await();

        CountDownLatch getLatch1 = new CountDownLatch(1);
        persistenceService.getResponse(id1, null, new LatchedActionListener<>(ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                assertEquals(model1, r), e -> {
            logger.debug("expect successful get result, got", e);
            fail("Expected successful get result, got " + e.getMessage());
        }), getLatch1));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, null, new LatchedActionListener<>(ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                assertEquals(model2, r), e -> {
            logger.debug("expect successful create, got", e);
            fail("Expected successful create, got " + e.getMessage());
        }), getLatch2));
        getLatch2.await();
    }

    public void testUpdateExpiration() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        ThreadPool threadPool1 = getInstanceFromNode(ThreadPool.class);
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User originalUser : Arrays.asList(user1, null)) {
            try (ThreadContext.StoredContext ctx = threadPool1.getThreadContext().stashContext()) {
                threadPool1.getThreadContext().putTransient(
                        ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT, getUserRolesString(originalUser));
                AsyncSearchResponse asyncSearchResponse = submitAndGetPersistedAsyncSearchResponse();
                long newExpirationTime = System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis();
                final AsyncSearchPersistenceModel newPersistenceModel = new AsyncSearchPersistenceModel(
                        asyncSearchResponse.getStartTimeMillis(),
                        newExpirationTime, asyncSearchResponse.getSearchResponse(), null, originalUser);

                for (User currentUser : Arrays.asList(user2, user1, null)) {
                    CountDownLatch updateLatch = new CountDownLatch(1);
                    if (originalUser != null && currentUser != null && currentUser.equals(originalUser) == false) {
                        ActionListener<AsyncSearchPersistenceModel> updateListener = ActionListener.wrap(
                                r -> fail("Expected security exception. Unauthorized update. Got " + r),
                                e -> assertTrue(e instanceof ElasticsearchSecurityException));
                        persistenceService.updateExpirationTime(asyncSearchResponse.getId(), newExpirationTime, currentUser,
                                new LatchedActionListener<>(updateListener, updateLatch));
                    } else {
                        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                                newExpirationTime, currentUser, new LatchedActionListener<>(
                                        ActionListener.wrap(persistenceModel -> assertEquals(newPersistenceModel, persistenceModel),
                                                e -> {
                                                    logger.debug("expect successful create, got", e);
                                                    fail("Expected successful create, got " + e.getMessage());
                                                }), updateLatch));
                    }
                    updateLatch.await();
                }
                CountDownLatch getLatch = new CountDownLatch(1);
                persistenceService.getResponse(asyncSearchResponse.getId(), originalUser, new LatchedActionListener<>(ActionListener.wrap(
                        r -> assertEquals(newPersistenceModel, r), e -> {
                            logger.debug("expect successful get result, got", e);
                            fail("Expected successful get result, got " + e.getMessage());
                        }), getLatch));
                getLatch.await();
            }
        }
    }

    public void testPersistenceServiceRetryTotalTime() {
        Iterator<TimeValue> times = AsyncSearchPersistenceService.STORE_BACKOFF_POLICY.iterator();
        long total = 0;
        while (times.hasNext()) {
            total += times.next().millis();
        }
        assertEquals(600000L, total);
    }

    public void testAsyncSearchExpirationUpdateOnBlockedPersistence() throws Exception {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsyncSearchId newAsyncSearchId = new AsyncSearchId(getInstanceFromNode(TransportService.class).getLocalNode().getId(), 1,
                asyncSearchContextId);
        String id = AsyncSearchIdConverter.buildAsyncId(newAsyncSearchId);
        AsyncSearchResponse mockResponse = new AsyncSearchResponse(id,
                AsyncSearchState.STORE_RESIDENT, randomNonNegativeLong(), randomNonNegativeLong(), getMockSearchResponse(), null);
        createDoc(getInstanceFromNode(AsyncSearchPersistenceService.class), mockResponse, null);
        client().admin().indices().prepareUpdateSettings(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build()).execute().actionGet();
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsyncSearchResponse asyncSearchResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);
        waitUntil(() -> verifyAsyncSearchState(client(), asyncSearchResponse.getId(), AsyncSearchState.PERSISTING));
        // This is needed to ensure we are able to acquire a permit for post processing before we try a GET operation
        waitUntil(() -> getRequestTimesOut(asyncSearchResponse.getId(), AsyncSearchState.PERSISTING));
        DeleteAsyncSearchRequest deleteAsyncSearchRequest = new DeleteAsyncSearchRequest(asyncSearchResponse.getId());
        try {
            executeDeleteAsyncSearch(client(), deleteAsyncSearchRequest).actionGet();
            fail("Expected timeout");
        } catch (Exception e) {
            assertThat(e, instanceOf(ElasticsearchTimeoutException.class));
        }
        client().admin().indices().prepareUpdateSettings(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build()).execute().actionGet();
        waitUntil(() -> verifyAsyncSearchState(client(), asyncSearchResponse.getId(), AsyncSearchState.STORE_RESIDENT));
        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteResponse(asyncSearchResponse.getId(), null,
                new LatchedActionListener<>(ActionListener.wrap(Assert::assertTrue, e -> fail("Unexpected failure " + e.getMessage()))
                        , deleteLatch));
        deleteLatch.await();
    }

    public void testDeleteExpiredResponse() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchResponse asyncSearchResponse = submitAndGetPersistedAsyncSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.currentTimeMillis() + new TimeValue(100, TimeUnit.MILLISECONDS).getMillis();
        final AsyncSearchPersistenceModel newPersistenceModel = new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                newExpirationTime, asyncSearchResponse.getSearchResponse(), null, null);
        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                newExpirationTime, null, new LatchedActionListener<>(
                        ActionListener.wrap(persistenceModel -> assertEquals(newPersistenceModel, persistenceModel),
                                e -> {
                                    logger.debug("expect successful create, got", e);
                                    fail("Expected successful update result, got " + e.getMessage());
                                }), updateLatch));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), null, new LatchedActionListener<>(
                ActionListener.wrap(r -> assertEquals(newPersistenceModel, r), e -> {
                    logger.debug("expect successful create, got", e);
                    fail("Expected successful create, got " + e.getMessage());
                }), getLatch));
        getLatch.await();

        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteExpiredResponses(new LatchedActionListener<>(new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                assertTrue(acknowledgedResponse.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Received exception while deleting expired response " + e.getMessage());
            }
        }, deleteLatch), System.currentTimeMillis());
        deleteLatch.await();
    }

    private void createDoc(AsyncSearchPersistenceService persistenceService, AsyncSearchResponse asyncSearchResponse, User user)
            throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.storeResponse(asyncSearchResponse.getId(),
                new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                        asyncSearchResponse.getExpirationTimeMillis(),
                        asyncSearchResponse.getSearchResponse(), null, user),
                new LatchedActionListener<>(
                        ActionListener.wrap(r -> assertSuccessfulResponseCreation(asyncSearchResponse.getId(), r), e -> {
                            logger.debug(() -> new ParameterizedMessage("Unexpected failure in  create due to "), e);
                        }), latch));
        latch.await();
    }

    private AsyncSearchResponse submitAndGetPersistedAsyncSearchResponse() throws InterruptedException {
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

    private void assertSuccessfulResponseCreation(String id, IndexResponse r) {
        assertSame(r.getResult(), DocWriteResponse.Result.CREATED);
        assertEquals(r.getId(), id);
    }

    public final String getUserRolesString(User user) {
        if (user == null) {
            return null;
        }
        return user.getName() + "|" + String.join(",", user.getBackendRoles()) + "|" + String.join(",", user.getRoles());
    }

    private boolean getRequestTimesOut(String id, AsyncSearchState state) {
        boolean timedOut;
        final GetAsyncSearchRequest getAsyncSearchRequest = new GetAsyncSearchRequest(id);
        getAsyncSearchRequest.setKeepAlive(TimeValue.timeValueHours(10));
        try {
            AsyncSearchResponse asyncSearchResponse = executeGetAsyncSearch(client(), getAsyncSearchRequest).actionGet();
            assertEquals(AsyncSearchState.PERSISTING, asyncSearchResponse.getState());
            timedOut = false;
        } catch (ElasticsearchTimeoutException e) {
            timedOut = true;
        }
        return timedOut;
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }
}
