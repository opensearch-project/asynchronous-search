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

public class SubmitAsynchronousSearchAction extends ActionType<AsynchronousSearchResponse> {

    public static final SubmitAsynchronousSearchAction INSTANCE = new SubmitAsynchronousSearchAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/submit";

    private SubmitAsynchronousSearchAction() {
        super(NAME, AsynchronousSearchResponse::new);
    }

}
