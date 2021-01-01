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
                    AsyncSearchState.PERSISTED,
                    asyncSearchResponse.getStartTimeMillis(),
                    asyncSearchResponse.getExpirationTimeMillis(),
                    asyncSearchResponse.getSearchResponse(),
                    asyncSearchResponse.getError());
            createDoc(persistenceService, newAsyncSearchResponse, user);

            if (user != null) {
                CountDownLatch getLatch1 = new CountDownLatch(1);
                persistenceService.getResponse(newAsyncSearchResponse.getId(), user2,
                        ActionListener.wrap(r -> failure(getLatch1, "Unauthorized get to the search result"),
                                e -> verifySecurityException(e, getLatch1)));
                getLatch1.await();
            }
            CountDownLatch getLatch2 = new CountDownLatch(1);
            persistenceService.getResponse(newAsyncSearchResponse.getId(), user,
                    ActionListener.wrap(r -> verifyPersistenceModel(
                            new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                                    asyncSearchResponse.getExpirationTimeMillis(), asyncSearchResponse.getSearchResponse(),
                                    null, user), r, getLatch2),
                            e -> failure(getLatch2, e)));
            getLatch2.await();
            if (user != null) {
                CountDownLatch deleteLatch1 = new CountDownLatch(1);
                User diffUser = TestClientUtils.randomUser();
                persistenceService.deleteResponse(newAsyncSearchResponse.getId(), user2,
                        ActionListener.wrap(r -> failure(deleteLatch1, "Unauthorized delete to the search result"),
                                e -> verifySecurityException(e, deleteLatch1)));
                deleteLatch1.await();
            }
            CountDownLatch deleteLatch2 = new CountDownLatch(1);
            persistenceService.deleteResponse(newAsyncSearchResponse.getId(), user,
                    ActionListener.wrap(r -> assertBoolean(deleteLatch2, r, true), e -> assertRnf(deleteLatch2, e)));
            deleteLatch2.await();

            //assert failure
            CountDownLatch getLatch3 = new CountDownLatch(2);
            persistenceService.getResponse(newAsyncSearchResponse.getId(), null,
                    ActionListener.wrap((AsyncSearchPersistenceModel r) -> failure(getLatch3,
                            new IllegalStateException("no response should " +
                                    "have been found for async search " + id)), exception -> assertRnf(getLatch3, exception)))
            ;
            persistenceService.getResponse(newAsyncSearchResponse.getId(), user2,
                    ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                            failure(getLatch3, new IllegalStateException("no response should " +
                                    "have been found for async search " + id)), exception -> assertRnf(getLatch3, exception)))
            ;
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
            persistenceService.storeResponse(id, model1, ActionListener.wrap(
                    r -> assertSuccessfulResponseCreation(id, r, createLatch), e -> failure(createLatch, e)));
            createLatch.await();
            for (User currentuser : Arrays.asList(originalUser, user2)) {
                CountDownLatch latch = new CountDownLatch(2);
                //assert failure
                persistenceService.getResponse("id", currentuser, ActionListener.wrap((AsyncSearchPersistenceModel r) -> failure(latch,
                        new IllegalStateException("no response should have been found for async search " + id)),
                        exception -> assertRnf(latch, exception)));
                //assert failure
                persistenceService.deleteResponse("id", currentuser,
                        ActionListener.wrap((r) -> assertBoolean(latch, r, false), e -> assertRnf(latch, e)));
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
                .execute(() -> persistenceService.storeResponse(id1, model1, ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id1, r, createLatch), e -> failure(createLatch, e))));
        threadPool.generic()
                .execute(() -> persistenceService.storeResponse(id2, model2, ActionListener.wrap(
                        r -> assertSuccessfulResponseCreation(id2, r, createLatch), e -> failure(createLatch, e))));
        createLatch.await();

        CountDownLatch getLatch1 = new CountDownLatch(1);
        persistenceService.getResponse(id1, null, ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                verifyPersistenceModel(model1, r, getLatch1), e -> failure(getLatch1, e)));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, null, ActionListener.wrap((AsyncSearchPersistenceModel r) ->
                verifyPersistenceModel(model2, r, getLatch2), e -> failure(getLatch2, e)));
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
                        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                                newExpirationTime, currentUser,
                                ActionListener.wrap(r -> failure(updateLatch, "Unauthorized update to the search result"),
                                        e -> verifySecurityException(e, updateLatch)));
                    } else {
                        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                                newExpirationTime, currentUser,
                                ActionListener.wrap(persistenceModel -> {
                                            verifyPersistenceModel(
                                                    newPersistenceModel,
                                                    persistenceModel,
                                                    updateLatch);
                                        },
                                        e -> failure(updateLatch, e)));
                    }
                    updateLatch.await();
                }
                CountDownLatch getLatch = new CountDownLatch(1);
                persistenceService.getResponse(asyncSearchResponse.getId(), originalUser, ActionListener.wrap(r -> {
                    verifyPersistenceModel(newPersistenceModel, r, getLatch);
                }, e -> failure(getLatch, e)));
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
                AsyncSearchState.PERSISTED, randomNonNegativeLong(), randomNonNegativeLong(), getMockSearchResponse(), null);
        createDoc(getInstanceFromNode(AsyncSearchPersistenceService.class), mockResponse, null);
        client().admin().indices().prepareUpdateSettings(AsyncSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build()).execute().actionGet();
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
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
        waitUntil(() -> verifyAsyncSearchState(client(), asyncSearchResponse.getId(), AsyncSearchState.PERSISTED));
        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteResponse(asyncSearchResponse.getId(), null,
                ActionListener.wrap(r -> assertBoolean(deleteLatch, r, true), e -> fail("Unexpected failure " + e.getMessage())));
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
                newExpirationTime, null,
                ActionListener.wrap(persistenceModel -> {
                            verifyPersistenceModel(
                                    newPersistenceModel,
                                    persistenceModel,
                                    updateLatch);
                        },
                        e -> failure(updateLatch, e)));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), null, ActionListener.wrap(r -> {
            verifyPersistenceModel(newPersistenceModel, r, getLatch);
        }, e -> failure(getLatch, e)));
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

    private void assertRnf(CountDownLatch latch, Exception exception) {
        try {
            assertThat(exception, instanceOf(ResourceNotFoundException.class));
        } finally {
            latch.countDown();
        }
    }

    private void failure(CountDownLatch latch, Exception e) {
        latch.countDown();
        fail(e.getMessage());
    }

    private void failure(CountDownLatch latch, String message) {
        latch.countDown();
        fail(message);
    }

    private void createDoc(AsyncSearchPersistenceService persistenceService, AsyncSearchResponse asyncSearchResponse, User user)
            throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.storeResponse(asyncSearchResponse.getId(),
                new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(),
                        asyncSearchResponse.getExpirationTimeMillis(),
                        asyncSearchResponse.getSearchResponse(), null, user),
                ActionListener.wrap(r -> assertSuccessfulResponseCreation(asyncSearchResponse.getId(), r, latch), e -> failure(latch, e)));
        latch.await();
    }

    private AsyncSearchResponse submitAndGetPersistedAsyncSearchResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = SubmitAsyncSearchRequest.getRequestWithDefaults(searchRequest);
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

    private void verifyPersistenceModel(AsyncSearchPersistenceModel expected, AsyncSearchPersistenceModel actual, CountDownLatch latch) {
        try {
            assertEquals(expected, actual);
        } finally {
            latch.countDown();
        }
    }

    private void verifySecurityException(Exception ex, CountDownLatch latch) {
        try {
            assertTrue(ex instanceof ElasticsearchSecurityException);
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
