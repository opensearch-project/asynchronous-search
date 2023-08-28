/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.service.persistence;

import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.search.asynchronous.commons.AsynchronousSearchSingleNodeTestCase;
import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.persistence.AsynchronousSearchPersistenceModel;
import org.opensearch.search.asynchronous.context.state.AsynchronousSearchState;
import org.opensearch.search.asynchronous.id.AsynchronousSearchId;
import org.opensearch.search.asynchronous.id.AsynchronousSearchIdConverter;
import org.opensearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.search.asynchronous.utils.TestClientUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.common.unit.TimeValue.timeValueDays;

public class AsynchronousSearchPersistenceServiceIT extends AsynchronousSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("persistenceServiceTests");
    }
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }
    public void testCreateAndGetAndDelete() throws IOException, InterruptedException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();

        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsynchronousSearchId newAsynchronousSearchId = new AsynchronousSearchId(transportService.getLocalNode().getId(),
                1, asContextId);
        String id = AsynchronousSearchIdConverter.buildAsyncId(newAsynchronousSearchId);
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User user : Arrays.asList(user1, null)) {
            AsynchronousSearchResponse newAsynchronousSearchResponse = new AsynchronousSearchResponse(id,
                    AsynchronousSearchState.STORE_RESIDENT,
                    asResponse.getStartTimeMillis(),
                    asResponse.getExpirationTimeMillis(),
                    asResponse.getSearchResponse(),
                    asResponse.getError());
            createDoc(persistenceService, newAsynchronousSearchResponse, user);

            if (user != null) {
                CountDownLatch getLatch1 = new CountDownLatch(1);
                ActionListener<AsynchronousSearchPersistenceModel> getListener = ActionListener.wrap(
                        r -> fail("Expected exception. Got " + r), e -> assertTrue(e instanceof OpenSearchSecurityException));
                persistenceService.getResponse(newAsynchronousSearchResponse.getId(), user2, new LatchedActionListener<>(getListener,
                        getLatch1));
                getLatch1.await();
            }
            CountDownLatch getLatch2 = new CountDownLatch(1);
            persistenceService.getResponse(newAsynchronousSearchResponse.getId(), user, new LatchedActionListener<>(
                    ActionListener.wrap(r -> assertEquals(
                            new AsynchronousSearchPersistenceModel(asResponse.getStartTimeMillis(),
                                    asResponse.getExpirationTimeMillis(), asResponse.getSearchResponse(),
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
                        e -> assertTrue(e instanceof OpenSearchSecurityException));
                persistenceService.deleteResponse(newAsynchronousSearchResponse.getId(), diffUser,
                        new LatchedActionListener<>(deleteListener, deleteLatch1));
                deleteLatch1.await();
            }
            CountDownLatch deleteLatch2 = new CountDownLatch(1);
            ActionListener<Boolean> deleteListener = ActionListener.wrap(Assert::assertTrue, e -> {
                logger.debug(() -> new ParameterizedMessage("Delete failed unexpectedly "), e);
                fail("delete failed.expected success");
            });
            persistenceService.deleteResponse(newAsynchronousSearchResponse.getId(), user,
                    new LatchedActionListener<>(deleteListener, deleteLatch2));
            deleteLatch2.await();

            //assert failure
            CountDownLatch getLatch3 = new CountDownLatch(2);
            ActionListener<AsynchronousSearchPersistenceModel> getListener = ActionListener.wrap((r) -> fail("Expected RNF, Got " + r),
                    exception -> assertTrue(exception instanceof ResourceNotFoundException));
            persistenceService.getResponse(newAsynchronousSearchResponse.getId(),
                    null, new LatchedActionListener<>(getListener, getLatch3));
            persistenceService.getResponse(newAsynchronousSearchResponse.getId(), user2,
                    new LatchedActionListener<>(getListener, getLatch3));
            getLatch3.await();
        }
    }

    public void testGetAndDeleteNonExistentId() throws InterruptedException, IOException, ExecutionException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User originalUser : Arrays.asList(user1, null)) {
            AsynchronousSearchId asId = generateNewAsynchronousSearchId(transportService);
            AsynchronousSearchPersistenceModel model1 = new AsynchronousSearchPersistenceModel(System.currentTimeMillis(),
                    System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(),
                    searchResponse, null, originalUser);
            CountDownLatch createLatch = new CountDownLatch(1);
            String id = AsynchronousSearchIdConverter.buildAsyncId(asId);
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
                        ActionListener.wrap(
                                (AsynchronousSearchPersistenceModel r) -> fail("Excepted resource_not_found_exception, got " + r),
                                exception -> assertTrue("Expected resource_not_found expection, got " +
                                                exception.getClass().toString(),
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
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        SearchResponse searchResponse = client().search(new SearchRequest(TEST_INDEX)).get();
        AsynchronousSearchId asId1 = generateNewAsynchronousSearchId(transportService);
        AsynchronousSearchId asId2 = generateNewAsynchronousSearchId(transportService);
        AsynchronousSearchPersistenceModel model1 = new AsynchronousSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse,
                null, null);
        String id1 = AsynchronousSearchIdConverter.buildAsyncId(asId1);

        AsynchronousSearchPersistenceModel model2 = new AsynchronousSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), searchResponse,
                null, null);
        String id2 = AsynchronousSearchIdConverter.buildAsyncId(asId2);
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
        persistenceService.getResponse(id1, null, new LatchedActionListener<>(ActionListener.wrap(
                (AsynchronousSearchPersistenceModel r) ->
                assertEquals(model1, r), e -> {
            logger.debug("expect successful get result, got", e);
            fail("Expected successful get result, got " + e.getMessage());
        }), getLatch1));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, null, new LatchedActionListener<>(ActionListener.wrap(
                (AsynchronousSearchPersistenceModel r) ->
                assertEquals(model2, r), e -> {
            logger.debug("expect successful create, got", e);
            fail("Expected successful create, got " + e.getMessage());
        }), getLatch2));
        getLatch2.await();
    }

    public void testUpdateExpiration() throws InterruptedException, IOException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        ThreadPool threadPool1 = getInstanceFromNode(ThreadPool.class);
        User user1 = TestClientUtils.randomUser();
        User user2 = TestClientUtils.randomUser();
        for (User originalUser : Arrays.asList(user1, null)) {
            try (ThreadContext.StoredContext ctx = threadPool1.getThreadContext().stashContext()) {
                threadPool1.getThreadContext().putTransient(
                        ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, getUserRolesString(originalUser));
                AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();
                long newExpirationTime = System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis();
                final AsynchronousSearchPersistenceModel newPersistenceModel = new AsynchronousSearchPersistenceModel(
                        asResponse.getStartTimeMillis(),
                        newExpirationTime, asResponse.getSearchResponse(), null, originalUser);

                for (User currentUser : Arrays.asList(user2, user1, null)) {
                    CountDownLatch updateLatch = new CountDownLatch(1);
                    if (originalUser != null && currentUser != null && currentUser.equals(originalUser) == false) {
                        ActionListener<AsynchronousSearchPersistenceModel> updateListener = ActionListener.wrap(
                                r -> fail("Expected security exception. Unauthorized update. Got " + r),
                                e -> assertTrue(e instanceof OpenSearchSecurityException));
                        persistenceService.updateExpirationTime(asResponse.getId(), newExpirationTime, currentUser,
                                new LatchedActionListener<>(updateListener, updateLatch));
                    } else {
                        persistenceService.updateExpirationTime(asResponse.getId(),
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
                persistenceService.getResponse(asResponse.getId(), originalUser, new LatchedActionListener<>(ActionListener.wrap(
                        r -> assertEquals(newPersistenceModel, r), e -> {
                            logger.debug("expect successful get result, got", e);
                            fail("Expected successful get result, got " + e.getMessage());
                        }), getLatch));
                getLatch.await();
            }
        }
    }

    public void testPersistenceServiceRetryTotalTime() {
        Iterator<TimeValue> times = AsynchronousSearchPersistenceService.STORE_BACKOFF_POLICY.iterator();
        long total = 0;
        while (times.hasNext()) {
            total += times.next().millis();
        }
        assertEquals(600000L, total);
    }


    public void testCreateResponseFailureOnClusterBlock() throws Exception {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsynchronousSearchId newAsynchronousSearchId = new AsynchronousSearchId(getInstanceFromNode(TransportService.class)
                .getLocalNode().getId(), 1, asContextId);
        String id = AsynchronousSearchIdConverter.buildAsyncId(newAsynchronousSearchId);
        AsynchronousSearchResponse mockResponse = new AsynchronousSearchResponse(id,
                AsynchronousSearchState.STORE_RESIDENT, randomNonNegativeLong(), randomNonNegativeLong(), getMockSearchResponse(), null);
        createDoc(getInstanceFromNode(AsynchronousSearchPersistenceService.class), mockResponse, null);
        client().admin().indices().prepareUpdateSettings(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build())
                .execute().actionGet();
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(5000));
        AsynchronousSearchResponse asResponse = TestClientUtils.blockingSubmitAsynchronousSearch(client(), request);
        waitUntil(() -> assertRnf(() -> TestClientUtils.blockingGetAsynchronousSearchResponse(client(),
                new GetAsynchronousSearchRequest(asResponse.getId()))));
        assertRnf(() -> TestClientUtils.blockingGetAsynchronousSearchResponse(client(), new GetAsynchronousSearchRequest(id)));
        client().admin().indices().prepareUpdateSettings(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX)
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build()).execute().actionGet();
    }

    public void testDeleteExpiredResponse() throws InterruptedException, IOException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.currentTimeMillis() + new TimeValue(100, TimeUnit.MILLISECONDS).getMillis();
        final AsynchronousSearchPersistenceModel newPersistenceModel = new AsynchronousSearchPersistenceModel(
                asResponse.getStartTimeMillis(),
                newExpirationTime, asResponse.getSearchResponse(), null, null);
        persistenceService.updateExpirationTime(asResponse.getId(),
                newExpirationTime, null, new LatchedActionListener<>(
                        ActionListener.wrap(persistenceModel -> assertEquals(newPersistenceModel, persistenceModel),
                                e -> {
                                    logger.debug("expect successful create, got", e);
                                    fail("Expected successful update result, got " + e.getMessage());
                                }), updateLatch));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asResponse.getId(), null, new LatchedActionListener<>(
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

    public void testConcurrentDeletes() throws InterruptedException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();

        int numThreads = 100;
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger numSuccess = new AtomicInteger();
        AtomicInteger numRnf = new AtomicInteger();
        AtomicInteger numFailure = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                persistenceService.deleteResponse(asResponse.getId(), null, new LatchedActionListener<>(ActionListener.wrap(
                        r -> {
                            if (r) {
                                numSuccess.getAndIncrement();
                            } else {
                                numFailure.getAndIncrement();
                            }
                        }, e -> {
                            if (e instanceof ResourceNotFoundException) {
                                numRnf.getAndIncrement();
                            } else {
                                numFailure.getAndIncrement();
                            }
                        }), latch));
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        latch.await();
        assertEquals(numSuccess.get(), 1);
        assertEquals(numFailure.get(), 0);
        assertEquals(numRnf.get(), numThreads - 1);
        for (Thread t : threads) {
            t.join();
        }
    }

    public void testConcurrentUpdates() throws InterruptedException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();
        int numThreads = 100;
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger numSuccess = new AtomicInteger();
        AtomicInteger numNoOp = new AtomicInteger();
        AtomicInteger numVersionConflictException = new AtomicInteger();
        AtomicInteger numFailure = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(() -> {
                long expirationTimeMillis = System.currentTimeMillis() + timeValueDays(10).millis();
                persistenceService.updateExpirationTime(asResponse.getId(),
                        expirationTimeMillis, null,
                        new LatchedActionListener<>(ActionListener.wrap(
                                r -> {
                                    if (r.getExpirationTimeMillis() == expirationTimeMillis) {
                                        numSuccess.getAndIncrement();
                                    } else if (r.getExpirationTimeMillis() == asResponse.getExpirationTimeMillis()) {
                                        numNoOp.getAndIncrement();
                                    } else {
                                        numFailure.getAndIncrement();
                                    }
                                }, e -> {
                                    if (e instanceof VersionConflictEngineException) {
                                        numVersionConflictException.getAndIncrement();
                                    } else {
                                        numFailure.getAndIncrement();
                                    }
                                }), latch));
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        latch.await();
        assertEquals(numFailure.get(), 0);
        assertEquals(numVersionConflictException.get() + numSuccess.get() + numNoOp.get(), numThreads);
        for (Thread t : threads) {
            t.join();
        }
        executeDeleteAsynchronousSearch(client(), new DeleteAsynchronousSearchRequest(asResponse.getId())).actionGet();
    }

    public void testConcurrentUpdatesAndDeletesRace() throws InterruptedException {
        AsynchronousSearchPersistenceService persistenceService = getInstanceFromNode(AsynchronousSearchPersistenceService.class);
        AsynchronousSearchResponse asResponse = submitAndGetPersistedAsynchronousSearchResponse();
        int numThreads = 200;
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger numDelete = new AtomicInteger();
        AtomicInteger numFailure = new AtomicInteger();
        AtomicInteger numDeleteAttempts = new AtomicInteger();
        AtomicInteger numDeleteFailedAttempts = new AtomicInteger();
        for (int i = 0; i < numThreads; i++) {
            final int iter = i;
            Thread t = new Thread(() -> {

                if (iter % 2 == 0 || iter < 20) /*letting few updates to queue up before starting to fire deletes*/ {
                    long expirationTimeMillis = System.currentTimeMillis() + timeValueDays(10).millis();
                    persistenceService.updateExpirationTime(asResponse.getId(),
                            expirationTimeMillis, null,
                            new LatchedActionListener<>(ActionListener.wrap(
                                    r -> {
                                        if (r.getExpirationTimeMillis() != expirationTimeMillis
                                                && r.getExpirationTimeMillis() != asResponse.getExpirationTimeMillis()) {
                                            numFailure.getAndIncrement();
                                        }
                                    }, e -> {
                                        // only version conflict from a concurrent update or RNF due to a concurrent delete is acceptable.
                                        // rest all failures are unexpected
                                        if (!(e instanceof VersionConflictEngineException) && !(e instanceof ResourceNotFoundException)) {
                                            numFailure.getAndIncrement();
                                        }
                                    }), latch));
                } else {
                    numDeleteAttempts.getAndIncrement();
                    persistenceService.deleteResponse(asResponse.getId(), null, new LatchedActionListener<>(ActionListener.wrap(
                            r -> {
                                if (r) {
                                    numDelete.getAndIncrement();
                                } else {
                                    numFailure.getAndIncrement();
                                }
                            }, e -> {
                                //only a failure due to concurrent delete causing RNF or concurrent update causing IllegalState is
                                // acceptable. rest all failures are unexpected
                                if (e instanceof ResourceNotFoundException || e instanceof IllegalStateException) {
                                    numDeleteFailedAttempts.getAndIncrement();
                                } else {
                                    numFailure.getAndIncrement();
                                }
                            }), latch));
                }
            });
            threads.add(t);
        }
        threads.forEach(Thread::start);
        latch.await();
        assertEquals(numFailure.get(), 0);
        assertEquals(numDeleteAttempts.get() - 1, numDeleteFailedAttempts.get());
        assertEquals(numDelete.get(), 1);
        for (Thread t : threads) {
            t.join();
        }
        expectThrows(ResourceNotFoundException.class, () -> executeDeleteAsynchronousSearch(client(),
                new DeleteAsynchronousSearchRequest(asResponse.getId())).actionGet());
    }

    private void createDoc(AsynchronousSearchPersistenceService persistenceService, AsynchronousSearchResponse asResponse, User user)
            throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.storeResponse(asResponse.getId(),
                new AsynchronousSearchPersistenceModel(asResponse.getStartTimeMillis(),
                        asResponse.getExpirationTimeMillis(),
                        asResponse.getSearchResponse(), null, user),
                new LatchedActionListener<>(
                        ActionListener.wrap(r -> assertSuccessfulResponseCreation(asResponse.getId(), r), e -> {
                            logger.debug(() -> new ParameterizedMessage("Unexpected failure in  create due to "), e);
                        }), latch));
        latch.await();
    }

    private AsynchronousSearchResponse submitAndGetPersistedAsynchronousSearchResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsynchronousSearchRequest request = new SubmitAsynchronousSearchRequest(searchRequest);
        request.keepOnCompletion(true);
        request.waitForCompletionTimeout(TimeValue.timeValueMillis(1));
        AsynchronousSearchResponse asResponse = TestClientUtils.blockingSubmitAsynchronousSearch(client(), request);
        TestClientUtils.assertResponsePersistence(client(), asResponse.getId());
        return TestClientUtils.blockingGetAsynchronousSearchResponse(client(), new GetAsynchronousSearchRequest(asResponse.getId()));
    }

    private boolean assertRnf(Runnable runnable) {
        try {
            runnable.run();
            return false;
        } catch (ResourceNotFoundException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    private AsynchronousSearchId generateNewAsynchronousSearchId(TransportService transportService) {
        AsynchronousSearchContextId asContextId = new AsynchronousSearchContextId(UUIDs.base64UUID(), randomInt(100));
        return new AsynchronousSearchId(transportService.getLocalNode().getId(), randomInt(100), asContextId);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 1, TimeUnit.SECONDS);
    }

    @After
    public void deleteAsynchronousSearchIndex() throws InterruptedException {
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

    private boolean getRequestTimesOut(String id, AsynchronousSearchState state) {
        boolean timedOut;
        final GetAsynchronousSearchRequest getAsynchronousSearchRequest = new GetAsynchronousSearchRequest(id);
        getAsynchronousSearchRequest.setKeepAlive(TimeValue.timeValueHours(10));
        try {
            AsynchronousSearchResponse asResponse = executeGetAsynchronousSearch(client(), getAsynchronousSearchRequest).actionGet();
            assertEquals(AsynchronousSearchState.PERSISTING, asResponse.getState());
            timedOut = false;
        } catch (OpenSearchTimeoutException e) {
            timedOut = true;
        }
        return timedOut;
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }
}
