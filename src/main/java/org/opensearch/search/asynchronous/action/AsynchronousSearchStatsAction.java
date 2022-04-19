/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.action;

import org.opensearch.search.asynchronous.response.AsynchronousSearchStatsResponse;
import org.opensearch.action.ActionType;
import org.opensearch.common.io.stream.Writeable;

public class AsynchronousSearchStatsAction extends ActionType<AsynchronousSearchStatsResponse> {


    public static final AsynchronousSearchStatsAction INSTANCE = new AsynchronousSearchStatsAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/stats";

    private AsynchronousSearchStatsAction() {
        super(NAME, AsynchronousSearchStatsResponse::new);
    }

    @Override
    public Writeable.Reader<AsynchronousSearchStatsResponse> getResponseReader() {
        return AsynchronousSearchStatsResponse::new;
    }
}
