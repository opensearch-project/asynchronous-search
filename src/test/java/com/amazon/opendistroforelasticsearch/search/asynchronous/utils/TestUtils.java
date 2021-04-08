package com.amazon.opendistroforelasticsearch.search.asynchronous.utils;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.opensearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

public class TestUtils {

    public static ClusterService createClusterService(Settings settings, ThreadPool threadPool, DiscoveryNode localNode,
                                                      ClusterSettings clusterSettings) {
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(TestUtils.class.getSimpleName()))
                .nodes(DiscoveryNodes.builder()
                        .add(localNode)
                        .localNodeId(localNode.getId())
                        .masterNodeId(localNode.getId()))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getMasterService().setClusterStatePublisher(
                createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static Map<String, Object> getResponseAsMap(SearchResponse searchResponse) throws IOException {
        if (searchResponse != null) {
            BytesReference response = XContentHelper.toXContent(searchResponse, Requests.INDEX_CONTENT_TYPE, true);
            if (response == null) {
                return emptyMap();
            }
            return convertToMap(response, false, Requests.INDEX_CONTENT_TYPE).v2();
        } else {
            return null;
        }
    }
}
