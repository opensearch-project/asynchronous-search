/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.utils;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.PluginSubject;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.security.Principal;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.nullValue;

public class PluginClientTests extends OpenSearchTestCase {

    private static final String CALLER_HEADER = "caller";
    private static final String PLUGIN_HEADER = "plugin";

    private static final ActionType<TestResponse> TEST_ACTION = new ActionType<>("cluster:admin/test", in -> new TestResponse());

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testRunsAsSubjectAndRestoresCallerContextForListener() {
        AtomicReference<String> callerSeenByDelegate = new AtomicReference<>();
        AtomicReference<String> pluginSeenByDelegate = new AtomicReference<>();
        AtomicReference<String> callerSeenByListener = new AtomicReference<>();

        NoOpClient delegate = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                callerSeenByDelegate.set(threadPool.getThreadContext().getHeader(CALLER_HEADER));
                pluginSeenByDelegate.set(threadPool.getThreadContext().getHeader(PLUGIN_HEADER));
                listener.onResponse(null);
            }
        };
        PluginClient pluginClient = new PluginClient(delegate);
        pluginClient.setSubject(new StashingSubject(threadPool));

        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(CALLER_HEADER, "user");
            pluginClient.execute(
                TEST_ACTION,
                new TestRequest(),
                ActionListener.wrap(
                    response -> callerSeenByListener.set(threadPool.getThreadContext().getHeader(CALLER_HEADER)),
                    e -> fail("unexpected failure: " + e)
                )
            );
        }

        assertThat(callerSeenByDelegate.get(), nullValue());
        assertEquals(PLUGIN_HEADER, pluginSeenByDelegate.get());
        // The listener is the caller's code, so it has to see the caller's context and not the subject's.
        assertEquals("user", callerSeenByListener.get());
    }

    public void testFailureInsideRunAsIsReportedThroughListener() {
        RuntimeException thrown = new RuntimeException("boom");
        AtomicReference<Exception> reported = new AtomicReference<>();

        PluginClient pluginClient = new PluginClient(new NoOpClient(threadPool));
        pluginClient.setSubject(new StashingSubject(threadPool) {
            @Override
            public <E extends Exception> void runAs(CheckedRunnable<E> r) {
                throw thrown;
            }
        });

        pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(response -> fail("expected a failure"), reported::set));

        assertSame(thrown, reported.get());
    }

    public void testWithoutASubjectTheClientRefusesToExecute() {
        PluginClient pluginClient = new PluginClient(new NoOpClient(threadPool));
        expectThrows(
            IllegalStateException.class,
            () -> pluginClient.execute(TEST_ACTION, new TestRequest(), ActionListener.wrap(r -> {}, failure -> {}))
        );
    }

    /**
     * Mirrors the real subject implementations, which stash the caller's context for the duration of runAs and
     * therefore restore it themselves on the way out. A fake without that behaviour would let a client that
     * never restores the caller's context for its listener pass.
     */
    private static class StashingSubject implements PluginSubject {

        private final ThreadPool threadPool;

        StashingSubject(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public Principal getPrincipal() {
            return new NamedPrincipal("plugin:test");
        }

        @Override
        public <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
            try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader(PLUGIN_HEADER, PLUGIN_HEADER);
                r.run();
            }
        }
    }

    private static class TestRequest extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    private static class TestResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
