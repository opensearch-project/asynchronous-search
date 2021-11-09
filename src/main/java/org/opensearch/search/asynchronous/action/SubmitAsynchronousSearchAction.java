/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.action;

import org.opensearch.search.asynchronous.response.AsynchronousSearchResponse;
import org.opensearch.action.ActionType;

public class SubmitAsynchronousSearchAction extends ActionType<AsynchronousSearchResponse> {

    public static final SubmitAsynchronousSearchAction INSTANCE = new SubmitAsynchronousSearchAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/submit";

    private SubmitAsynchronousSearchAction() {
        super(NAME, AsynchronousSearchResponse::new);
    }

}
