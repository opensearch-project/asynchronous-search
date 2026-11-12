/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.plugin;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.asynchronous.service.AsynchronousSearchPersistenceService;
import org.opensearch.search.asynchronous.utils.RecordingSubject;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/** Tests that the plugin wires the subject core assigns into the client its services use. */
public class AsynchronousSearchPluginTests extends OpenSearchTestCase {

    public void testPersistenceServiceRunsAsTheAssignedSubject() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        AsynchronousSearchPlugin plugin = new AsynchronousSearchPlugin();
        Set<Setting<?>> settings = Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), plugin.getSettings().stream())
            .collect(Collectors.toSet());
        DiscoveryNode node = new DiscoveryNode(
            "node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            node,
            new ClusterSettings(Settings.EMPTY, settings)
        );
        try {
            ClusterServiceUtils.setState(
                clusterService,
                ClusterStateCreationUtils.stateWithActivePrimary(AsynchronousSearchPersistenceService.ASYNC_SEARCH_RESPONSE_INDEX, true, 0)
            );

            AtomicReference<String> subjectSeenByDelegate = new AtomicReference<>("action did not run");
            NoOpClient client = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    subjectSeenByDelegate.set(threadPool.getThreadContext().getTransient(RecordingSubject.SUBJECT_TRANSIENT));
                }
            };
            Collection<Object> components = plugin.createComponents(
                client,
                clusterService,
                threadPool,
                null,
                null,
                null,
                null,
                null,
                new NamedWriteableRegistry(emptyList()),
                null,
                null
            );
            plugin.assignSubject(new RecordingSubject(threadPool.getThreadContext(), "set-by-subject"));

            AsynchronousSearchPersistenceService persistenceService = components.stream()
                .filter(AsynchronousSearchPersistenceService.class::isInstance)
                .map(AsynchronousSearchPersistenceService.class::cast)
                .findFirst()
                .orElseThrow();
            persistenceService.getResponse("id", null, ActionListener.wrap(r -> {}, e -> {}));

            assertEquals(
                "the persistence service must use the client that holds the assigned subject",
                "set-by-subject",
                subjectSeenByDelegate.get()
            );
        } finally {
            clusterService.close();
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
