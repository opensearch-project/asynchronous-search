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

package com.amazon.opendistroforelasticsearch.search.asynchronous.context.permits;

import com.amazon.opendistroforelasticsearch.search.asynchronous.context.AsynchronousSearchContextId;
import com.amazon.opendistroforelasticsearch.search.asynchronous.context.active.AsynchronousSearchContextClosedException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class NoopAsynchronousSearchContextPermitsTests extends OpenSearchTestCase {

    public void testAcquireAllPermits() {
        NoopAsynchronousSearchContextPermits permits = new NoopAsynchronousSearchContextPermits(
                new AsynchronousSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong()));
        expectThrows(IllegalStateException.class,
                () -> permits.asyncAcquireAllPermits(ActionListener.wrap(Assert::fail), TimeValue.ZERO, "reason"));
    }

    public void testAcquireSinglePermit() throws InterruptedException {
        NoopAsynchronousSearchContextPermits permits = new NoopAsynchronousSearchContextPermits(
                new AsynchronousSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        permits.asyncAcquirePermit(new LatchedActionListener<>(ActionListener.wrap(r -> {},
                e -> fail("expected permit acquisition to succeed")), countDownLatch), TimeValue.ZERO, "reason");
        countDownLatch.await();
    }

    public void testAcquireSinglePermitAfterClosure() throws InterruptedException {
        AsynchronousSearchContextId contextId = new AsynchronousSearchContextId(UUID.randomUUID().toString(), randomNonNegativeLong());
        NoopAsynchronousSearchContextPermits permits = new NoopAsynchronousSearchContextPermits(
                contextId);
        permits.close();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        permits.asyncAcquirePermit(new LatchedActionListener<>(ActionListener.wrap(
                r -> fail("expected permit acquisition to fail due to permit closure"),
                e -> {
                    assertTrue("expected context closed exception, got " + e.getClass(),
                            e instanceof AsynchronousSearchContextClosedException);
                    assertTrue(((AsynchronousSearchContextClosedException) e).getAsynchronousSearchContextId().equals(contextId));
                }), countDownLatch), TimeValue.ZERO, "reason");
        countDownLatch.await();
    }

}
