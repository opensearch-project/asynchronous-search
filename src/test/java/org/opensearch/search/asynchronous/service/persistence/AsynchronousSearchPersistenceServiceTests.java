/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.service.persistence;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Tests the requests the persistence service issues, without a cluster. */
public class AsynchronousSearchPersistenceServiceTests extends OpenSearchTestCase {

    public void testGetResponseAsksForARefreshSoANonRealtimeGetSeesTheStoredResponse() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            ClusterServiceUtils.setState(
                clusterService,
                ClusterStateCreationUtils.stateWithActivePrimary(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX, true, 0)
            );
            AtomicReference<ActionRequest> issued = new AtomicReference<>();
            NoOpClient client = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    issued.set(request);
                }
            };
            AsynchronousSearchPersistenceService persistenceService = new AsynchronousSearchPersistenceService(
                client,
                clusterService,
                threadPool
            );

            persistenceService.getResponse("id", null, ActionListener.wrap(r -> {}, e -> {}));

            GetRequest getRequest = (GetRequest) issued.get();
            assertNotNull("getResponse must issue a GET", getRequest);
            assertTrue("realtime is still requested; the security plugin is what turns it off", getRequest.realtime());
            // Core acts on refresh only once realtime is off, which is when a write just made would be missed.
            assertTrue("a non-realtime GET must refresh first to see a response stored a moment ago", getRequest.refresh());
        } finally {
            clusterService.close();
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
