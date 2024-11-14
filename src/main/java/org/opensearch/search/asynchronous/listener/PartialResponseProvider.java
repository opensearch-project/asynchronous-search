/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.listener;

import org.opensearch.action.search.SearchResponse;

@FunctionalInterface
public interface PartialResponseProvider {

    SearchResponse partialResponse();
}
