/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.asynchronous.utils;

import org.opensearch.ResourceNotFoundException;

import java.util.Locale;

public class AsynchronousSearchExceptionUtils {

    public static ResourceNotFoundException buildResourceNotFoundException(String id) {
        return new ResourceNotFoundException(
            String.format(Locale.ROOT, "Either the resource [%s] does not exist or you do not have access", id)
        );
    }
}
