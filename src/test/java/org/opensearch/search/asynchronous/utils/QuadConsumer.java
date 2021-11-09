/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.search.asynchronous.utils;


/**
 * Represents an operation that accepts four arguments and returns no result.
 *
 * @param <S> the type of the first argument
 * @param <T> the type of the second argument
 * @param <U> the type of the third argument
 * @param <V> the type of the fourth argument
 */
@FunctionalInterface
public interface QuadConsumer<S, T, U, V> {
    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @param v the fourth function argument
     */
    void apply(S s, T t, U u, V v);
}
