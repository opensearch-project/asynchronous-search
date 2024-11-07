/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.task;

import org.opensearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.core.tasks.TaskId;

import java.util.Map;

/**
 * Task storing information about a currently running {@link SubmitAsynchronousSearchRequest}.
 */
public class SubmitAsynchronousSearchTask extends CancellableTask {

    public SubmitAsynchronousSearchTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

}
