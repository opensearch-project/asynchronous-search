/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.listener;

import org.opensearch.action.search.SearchResponse;

@FunctionalInterface
public interface PartialResponseProvider {

    SearchResponse partialResponse();
}
