/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.context.permits;

import org.opensearch.search.asynchronous.context.AsynchronousSearchContextId;
import org.opensearch.search.asynchronous.context.active.AsynchronousSearchContextClosedException;
import org.opensearch.core.action.ActionListener;
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
