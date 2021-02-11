## 2021-02-03 Version 1.13.0.0
Initial launch of Asynchronous Search on opendistro-for-elasticsearch ([RFC](https://github.com/opendistro-for-elasticsearch/community/files/5618042/RFC_.Asynchronous.Search.with.Elasticsearch.1.pdf))

Occasionally business would like to run queries on vast amount of data that can take very long to return results.

Asynchronous search makes it possible for users to run such queries without worrying about the query timing out. These queries run in the background, and users can track the progress, and retrieve results as they become available.

The asynchronous search APIs let you asynchronously execute a search request, monitor its progress, and retrieve partial results as they become available.

### Maintenance
* Supports Elasticsearch 7.10.2 ([#32](https://github.com/opendistro-for-elasticsearch/asynchronous-search/pull/32))


### Features
* Listener framework for asynchronous search ([#2](https://github.com/opendistro-for-elasticsearch/asynchronous-search/pull/2))
* Service layer and Transport Handlers ([#8](https://github.com/opendistro-for-elasticsearch/asynchronous-search/pull/8))
* Rest Layer and Asynchronous Search Cleanup Management ([#9](https://github.com/opendistro-for-elasticsearch/asynchronous-search/pull/9))
* Integrates security with asynchronous search ([#11](https://github.com/opendistro-for-elasticsearch/asynchronous-search/pull/11))
