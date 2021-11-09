/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.action;

import org.opensearch.search.asynchronous.response.AcknowledgedResponse;
import org.opensearch.action.ActionType;

public class DeleteAsynchronousSearchAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteAsynchronousSearchAction INSTANCE = new DeleteAsynchronousSearchAction();
    public static final String NAME = "cluster:admin/opendistro/asynchronous_search/delete";

    private DeleteAsynchronousSearchAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
