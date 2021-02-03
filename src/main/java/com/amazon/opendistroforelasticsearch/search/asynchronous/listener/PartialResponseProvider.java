package com.amazon.opendistroforelasticsearch.search.asynchronous.listener;

import org.elasticsearch.action.search.SearchResponse;

@FunctionalInterface
public interface PartialResponseProvider {

    SearchResponse partialResponse();
}
