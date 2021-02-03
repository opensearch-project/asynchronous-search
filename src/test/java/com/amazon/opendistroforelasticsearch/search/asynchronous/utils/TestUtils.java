package com.amazon.opendistroforelasticsearch.search.asynchronous.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterStatePublisher;
import static org.elasticsearch.test.ClusterServiceUtils.createNoOpNodeConnectionsService;

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
