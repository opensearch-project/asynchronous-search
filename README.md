[![Test and Build Workflow](https://github.com/opensearch-project/asynchronous-search/workflows/Test%20and%20Build%20Workflow/badge.svg)](https://github.com/opensearch-project/asynchronous-search/actions)
[![codecov](https://codecov.io/gh/opensearch-project/asynchronous-search/branch/main/graph/badge.svg)](https://codecov.io/gh/opensearch-project/asynchronous-search)

# Asynchronous search
Asynchronous search makes it possible for users to run such queries without worrying about the query timing out. 
These queries run in the background, and users can track the progress, and retrieve partial results as they become available.

The asynchronous search plugin supports the below operations

**1. Submit asynchronous search**
```
POST /_plugins/_asynchronous_search?wait_for_completion_timeout=500ms&keep_on_completion=true&keep_alive=3d
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
GET /_plugins/_asynchronous_search/FjdITFhYbC1zVFdHVVV1MUd3UkxkMFEFMjQ1MzYUWHRrZjhuWUJXdFhxMmlCSW5HTE8BMQ==?keep_alive=3d
```

**3. Delete an asynchronous search**

```
DELETE /_plugins/_asynchronous_search/FjdITFhYbC1zVFdHVVV1MUd3UkxkMFEFMjQ1MzYUWHRrZjhuWUJXdFhxMmlCSW5HTE8BMQ==
```

**4. Stats for asynchronous search**

```
GET /_plugins/_asynchronous_search/stats
```

**Tunable Settings**
1. `plugins.asynchronous_search.max_search_running_time` : Maximum running time for the search beyond which the search would be terminated
2. `plugins.asynchronous_search.node_concurrent_running_searches` : Concurrent searches running per coordinator node
3. `plugins.asynchronous_search.max_keep_alive` : Maximum keep alive for search which dictates how long the search is allowed to be present in the cluster
4. `plugins.asynchronous_search.max_wait_for_completion_timeout` : Maximum keep on completion to block for the search response
5. `plugins.asynchronous_search.persist_search_failures` : Persist asynchronous search result ending with search failure in system index

## Setup

1. Check out this package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package. 
3. To build from the command line, set `JAVA_HOME` to point to a JDK >= 8 before running `./gradlew`.
  - Unix System
    1. `export JAVA_HOME=jdk-install-dir`: Replace `jdk-install-dir` with the JAVA_HOME directory of your system.
    2. `export PATH=$JAVA_HOME/bin:$PATH`
 
  - Windows System
    1. Find **My Computers** from file directory, right click and select **properties**.
    2. Select the **Advanced** tab, select **Environment variables**.
    3. Edit **JAVA_HOME** to path of where JDK software is installed.

## Build

The project in this package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.


### Building from the command line

1. `./gradlew build` builds and tests project.
2. `./gradlew run` launches a single node cluster with the asynchronous search plugin installed.
3. `./gradlew run -PnumNodes=3` launches a multi-node cluster with the asynchronous search plugin installed.
4. `./gradlew integTest` launches a single node cluster with the asynchronous search plugin installed and runs all integ tests.
5. `./gradlew integTest -PnumNodes=3` launches a multi-node cluster with the asynchronous search plugin installed and runs all integ tests.
6. `./gradlew integTest -Dtests.class=*AsynchronousSearchRestIT` runs a single integ class
7. `./gradlew integTest -Dtests.class=*AsynchronousSearchRestIT -Dtests.method="testSubmitWithRetainedResponse"` runs a single integ test method (remember to quote the test method name if it contains spaces)
8. `./gradlew asynSearchCluster#mixedClusterTask -Dtests.security.manager=false` launches a cluster of three nodes of bwc version of OpenSearch with async search plugin and tests backwards compatibility by performing rolling upgrade of one node with the current version of OpenSearch with async search plugin.
9. `./gradlew asynSearchCluster#rollingUpgradeClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with async search plugin and tests backwards compatibility by performing rolling upgrade of all nodes with the current version of OpenSearch with async search plugin.
10. `./gradlew asynSearchCluster#fullRestartClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with async search plugin and tests backwards compatibility by performing a full restart on the cluster upgrading all the nodes with the current version of OpenSearch with async search plugin.
11. `./gradlew bwcTestSuite -Dtests.security.manager=false` runs all the above bwc tests combined.
12. `./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="docker-cluster" -Dhttps=true -Duser=admin -Dpassword=<admin-password>` launches integration tests against a local cluster and run tests with security

When launching a cluster using one of the above commands, logs are placed in `build/testclusters/integTest-0/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Debugging

Sometimes it is useful to attach a debugger to either the OpenSearch cluster or the integ tests to see what's going on. When running unit tests, hit **Debug** from the IDE's gutter to debug the tests.  For the OpenSearch cluster or the integ tests, first, make sure start a debugger listening on port `5005`. 

To debug the server code, run:

```
./gradlew :integTest -Dcluster.debug # to start a cluster with debugger and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The OpenSearch server JVM will connect to a debugger attached to `localhost:5005`.

The IDE needs to listen for the remote JVM. If using Intellij you must set your debug configuration to "Listen to remote JVM" and make sure "Auto Restart" is checked.
You must start your debugger to listen for remote JVM before running the commands.

To debug code running in an integration test (which exercises the server from a separate JVM), first, setup a remote debugger listening on port `8000`, and then run:

```
./gradlew :integTest -Dtest.debug
```

The test runner JVM will connect to a debugger attached to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :integTest -Dtest.debug -Dcluster.debug
```


## Getting Help

If you find a bug, or have a feature request, please don't hesitate to open an issue in this repository.

For more information, see the [project website](https://opensearch.org/) and [technical documentation](https://opensearch.org/docs/latest/search-plugins/async/index/). If you need help and are unsure where to open an issue, try the OpenSearch [Forum](https://forum.opensearch.org/).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the [Apache v2.0 License](LICENSE).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE) for details.
