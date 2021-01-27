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

package com.amazon.opendistroforelasticsearch.search.asynchronous.utils;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.DeleteAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.GetAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.action.SubmitAsynchronousSearchAction;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.state.AsynchronousSearchState;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class TestClientUtils {
    static final String INDEX = AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX;
    static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(100), 20);

    public static AsynchronousSearchResponse blockingSubmitAsynchronousSearch(Client client, SubmitAsynchronousSearchRequest request) {
        ActionFuture<AsynchronousSearchResponse> execute = submitAsynchronousSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AsynchronousSearchResponse> submitAsynchronousSearch(Client client, SubmitAsynchronousSearchRequest request) {
        return client.execute(SubmitAsynchronousSearchAction.INSTANCE, request);
    }

    public static AsynchronousSearchResponse blockingGetAsynchronousSearchResponse(Client client, GetAsynchronousSearchRequest request) {
        ActionFuture<AsynchronousSearchResponse> execute = getAsynchronousSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AsynchronousSearchResponse> getAsynchronousSearch(Client client, GetAsynchronousSearchRequest request) {
        return client.execute(GetAsynchronousSearchAction.INSTANCE, request);
    }

    public static AcknowledgedResponse blockingDeleteAsynchronousSearchRequest(Client client, DeleteAsynchronousSearchRequest request) {
        ActionFuture<AcknowledgedResponse> execute = deleteAsynchronousSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AcknowledgedResponse> deleteAsynchronousSearch(Client client, DeleteAsynchronousSearchRequest request) {
        return client.execute(DeleteAsynchronousSearchAction.INSTANCE, request);
    }

    /**
     * Match with submit asynchronous search response.
     */
    static AsynchronousSearchResponse blockingGetAsynchronousSearchResponse(Client client, AsynchronousSearchResponse submitResponse,
                                                              GetAsynchronousSearchRequest getAsynchronousSearchRequest) {
        AsynchronousSearchResponse getResponse = blockingGetAsynchronousSearchResponse(client, getAsynchronousSearchRequest);
        assert getResponse.getId().equals(submitResponse.getId());
        assert getResponse.getStartTimeMillis() == submitResponse.getStartTimeMillis();
        return getResponse;
    }

    public static AsynchronousSearchResponse getFinalAsynchronousSearchResponse(Client client, AsynchronousSearchResponse submitResponse,
                                                                  GetAsynchronousSearchRequest getAsynchronousSearchRequest) {
        AsynchronousSearchResponse getResponse;
        do {
            getResponse = blockingGetAsynchronousSearchResponse(client, submitResponse, getAsynchronousSearchRequest);
        } while (getResponse.getState().equals(AsynchronousSearchState.RUNNING.name()));
        return getResponse;
    }

    public static void assertResponsePersistence(Client client, String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Iterator<TimeValue> backoff = STORE_BACKOFF_POLICY.iterator();
        getResponseFromIndex(client, id, latch, backoff);
        latch.await();
    }

    public static void getResponseFromIndex(Client client, String id, CountDownLatch latch, Iterator<TimeValue> backoff) {
        client.get(new GetRequest(INDEX).refresh(true).id(id), new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    latch.countDown();
                } else {
                    onFailure(new Exception("Get Response doesn't exist."));
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    if (backoff.hasNext() == false) {
                        latch.countDown();
                        Assert.fail("Failed to persist asynchronous search response");
                    } else {
                        TimeValue wait = backoff.next();
                        Thread.sleep(wait.getMillis());
                        getResponseFromIndex(client, id, latch, backoff);
                    }
                } catch (InterruptedException ex) {
                    Assert.fail();
                    latch.countDown();
                }
            }
        });
    }

    public static User randomUser() {
        return new User(ESRestTestCase.randomAlphaOfLength(10), Arrays.asList(
                ESRestTestCase.randomAlphaOfLength(10),
                ESRestTestCase.randomAlphaOfLength(10)),
                Arrays.asList(ESRestTestCase.randomAlphaOfLength(10), "all_access"), Arrays.asList());
    }

    public static User randomUserOrNull() {
        return Randomness.get().nextBoolean() ? randomUser() : null;
    }
}
