/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.test.ClusterServiceUtils;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.opensearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

public class TestUtils {

    public static ClusterService createClusterService(
        Settings settings,
        ThreadPool threadPool,
        DiscoveryNode localNode,
        ClusterSettings clusterSettings
    ) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(settings, clusterSettings, threadPool);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(TestUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).clusterManagerNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getClusterManagerService()
            .setClusterStatePublisher(createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getClusterManagerService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static Map<String, Object> getResponseAsMap(SearchResponse searchResponse) throws IOException {
        if (searchResponse != null) {
            BytesReference response = org.opensearch.core.xcontent.XContentHelper.toXContent(
                searchResponse,
                Requests.INDEX_CONTENT_TYPE,
                true
            );
            if (response == null) {
                return emptyMap();
            }
            return convertToMap(response, false, Requests.INDEX_CONTENT_TYPE).v2();
        } else {
            return null;
        }
    }
}
