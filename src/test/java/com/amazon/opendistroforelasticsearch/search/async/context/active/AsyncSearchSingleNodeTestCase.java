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

package com.amazon.opendistroforelasticsearch.search.async.context.active;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public abstract class AsyncSearchSingleNodeTestCase extends ESSingleNodeTestCase {
    static final String INDEX = ".async_search_response";
    static final String TEST_INDEX = "index";

    @Override
    protected boolean resetNodeAfterTest() {
        return false;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createIndex(TEST_INDEX, Settings.builder().put("index.refresh_interval", -1).build());
        for (int i = 0; i < 10; i++)
            client().prepareIndex(TEST_INDEX, "type", String.valueOf(i)).setSource("field", "value" + i)
                    .setRefreshPolicy(IMMEDIATE).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }
//
//    @Override
//    public void tearDown() throws Exception {
////        client().admin().indices().prepareDelete(INDEX).setTimeout(new TimeValue(10, TimeUnit.SECONDS)).get();
//        Thread.sleep(1000);
//        super.tearDown();
//    }

    <C extends Client,Req,R> void assertRNF(BiFunction<C,Req,R> function, C client, Req req) {
        try {
            function.apply(client, req);
        } catch (ResourceNotFoundException e) {
            return;
        }
        fail("Should have entered catch block");
    }
}
