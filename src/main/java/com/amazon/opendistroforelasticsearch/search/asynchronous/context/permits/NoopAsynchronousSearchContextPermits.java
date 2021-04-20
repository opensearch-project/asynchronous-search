/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchContextClosedException;
import org.opensearch.action.ActionListener;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;

/**
 * NOOP context permit that responds with a NOOP {@linkplain Releasable} to release
 */
public class NoopAsynchronousSearchContextPermits extends AsynchronousSearchContextPermits {

    public NoopAsynchronousSearchContextPermits(AsynchronousSearchContextId asynchronousSearchContextId) {
        super(asynchronousSearchContextId, null, null);
    }

    @Override
    public void asyncAcquirePermit(final ActionListener<Releasable> onAcquired, final TimeValue timeout, String reason) {
        if (closed) {
            logger.debug("Trying to acquire permit for closed context [{}]", asynchronousSearchContextId);
            onAcquired.onFailure(new AsynchronousSearchContextClosedException(asynchronousSearchContextId));
        } else {
            onAcquired.onResponse(() -> {});
        }
    }

    @Override
    public void asyncAcquireAllPermits(ActionListener<Releasable> onAcquired, TimeValue timeout, String reason) {
        throw new IllegalStateException("Acquiring all permits is not allowed for asynchronous search id" + asynchronousSearchContextId);
    }
}
