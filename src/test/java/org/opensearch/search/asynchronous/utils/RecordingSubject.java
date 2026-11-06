/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.utils;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.PluginSubject;

import java.security.Principal;

/**
 * Stands in for the plugin subject. Stashes and restores around its body as NoopPluginSubject and
 * SecurePluginSubject do, and marks the context so a test can tell the subject's apart from the caller's.
 */
public class RecordingSubject implements PluginSubject {

    /** Transient that {@link #runAs} sets to this subject's marker. */
    public static final String SUBJECT_TRANSIENT = "subject.marker";

    private final ThreadContext threadContext;
    private final String marker;

    /**
     * Creates a subject that marks the context it runs in.
     *
     * @param threadContext the context to stash and mark
     * @param marker the value {@link #SUBJECT_TRANSIENT} holds while the body runs
     */
    public RecordingSubject(ThreadContext threadContext, String marker) {
        this.threadContext = threadContext;
        this.marker = marker;
    }

    @Override
    public Principal getPrincipal() {
        return new NamedPrincipal("plugin:test");
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> runnable) throws E {
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putTransient(SUBJECT_TRANSIENT, marker);
            runnable.run();
        }
    }
}
