/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.action;

import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.ActionType;

public class GetAsynchronousSearchAction extends ActionType<AsynchronousSearchResponse> {

    public static final GetAsynchronousSearchAction INSTANCE = new GetAsynchronousSearchAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/get";

    private GetAsynchronousSearchAction() {
        super(NAME, AsynchronousSearchResponse::new);
    }

}
