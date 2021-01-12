[![Test and Build Workflow](https://github.com/opendistro-for-elasticsearch/index-management/workflows/Test%20and%20Build%20Workflow/badge.svg)](https://github.com/opendistro-for-elasticsearch/asynchronous-search/actions)

# Open Distro for Elasticsearch asynchronous search
Asynchronous search makes it possible for users to run such queries without worrying about the query timing out. 
These queries run in the background, and users can track the progress, and retrieve partial results as they become available.

The asynchronous search plugin supports the below operations

**1. Submit asynchronous search**
```
POST /_opendistro/_asynchronous_search?wait_for_completion_timeout=500ms&keep_on_completion=true&keep_alive=3d
{  "aggs": {
    "city": {
      "terms": {
        "field": "city", "size": 50
      }
    }
  }
}

```

**2. Retrieve asynchronous search results**
```
GET /_opendistro/_asynchronous_search/FjdITFhYbC1zVFdHVVV1MUd3UkxkMFEFMjQ1MzYUWHRrZjhuWUJXdFhxMmlCSW5HTE8BMQ==?keep_alive=3d
```

**3. Delete an asynchronous search**

```
DELETE /_opendistro/_asynchronous_search/FjdITFhYbC1zVFdHVVV1MUd3UkxkMFEFMjQ1MzYUWHRrZjhuWUJXdFhxMmlCSW5HTE8BMQ==
```

**4. Stats for asynchronous search**

```
GET /_opendistro/_asynchronous_search/_stats
```

**Tunable Settings**
1. `opendistro_asynchronous_search.max_search_running_time` : Maximum running time for the search beyond which the search would be terminated
2. `opendistro_asynchronous_search.max_running_searches` : Concurrent searches running per coordinator node
3. `opendistro_asynchronous_search.max_keep_alive` : Maximum keep alive for search which dictates how long the search is allowed to be present in the cluster
4. `opendistro_asynchronous_search.max_wait_for_completion_timeout` : Maximum keep on completion to block for the search response

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
