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
package com.amazon.opendistroforelasticsearch.search.asynchronous.action;

import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.elasticsearch.action.ActionType;

public class SubmitAsynchronousSearchAction extends ActionType<AsynchronousSearchResponse> {

    public static final SubmitAsynchronousSearchAction INSTANCE = new SubmitAsynchronousSearchAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/submit";

    private SubmitAsynchronousSearchAction() {
        super(NAME, AsynchronousSearchResponse::new);
    }

}
