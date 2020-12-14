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

package com.amazon.opendistroforelasticsearch.search.async.task;

import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * Task storing information about a currently running {@link SubmitAsyncSearchRequest}.
 */
public class SubmitAsyncSearchTask extends CancellableTask {

    public SubmitAsyncSearchTask(
            long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

}

