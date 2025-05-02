# oracle-nosql

Oracle NoSQL Database

Oracle NoSQL Database is designed for today’s most demanding applications that
require low latency responses, flexible data models, and elastic scaling for
dynamic workloads. It supports JSON, Table and Key-Value datatypes running
on-premise, or as a cloud service with on-demand throughput and storage based
provisioning.

Oracle NoSQL Database Cloud Service is now a fully managed database service
running on Gen 2 Oracle Cloud Infrastructure hardware.

## Prerequisites

- Java SE 11 or later installed on all Storage Nodes
  - Download and install a [Java](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
  binary release suitable for your system. See the install and setup
  instructions on that page.
- Maven 3.5.0 or later for build

## Build

The project consists of following modules

- kvmain (includes all source code. All other modules except kvtest fetch
  compiled classes from kvmain)
- kvstore (generates kvstore.jar)
- kvclient (generates kvclient.jar)
- sql (generates sql.jar)
- recovery (generates recovery.jar)
- kvtest (Parent module for all tests that run in `verify` phase)
  - kvstore-IT (All tests for kvstore, and all TestBase/Util classes, depends
    on kvstore.jar)
  - kvclient-IT (kvclient tests, depends on kvclient and kvstore-IT for compilation)
  - kvquery-IT (QTF tests, depends on kvstore, kvstore-IT and com.oracle.nosql.common:qtf)
  - kvdatacheck-IT (Datacheck unit test, depends on kvstore and kvstore-IT)
  - kvtif-IT (Text Index Feeder tests, depends on kvstore-IT)
  - coverage-report (Run jacoco coverage report)
- httpproxy (source code for NoSQL Httpproxy)
- packaging (generates shell jars and release packages)

To compile only the source code:

```bash
mvn [clean] compile -pl kvmain
```

`clean` is optional, it cleans the `target` folder before compile.

To compile with both source code and test code:

```bash
mvn [clean] compile
```

To build all KV artifacts and copy them with dependencies to a single folder:

```bash
mvn [clean] package
```

> [!NOTE] You can find the folder under `packaging/target/`. Above command will
build and package all modules, but it will skip tests execution by default.

## Running Tests

### Running tests for all modules

```bash
mvn -fn -PIT verify
```

The default test profile "IT" includes all test modules. It takes about 14-16
hours to run all tests.

### Running tests in sync mode

```bash
mvn -fn -PIT,sync verify
```

### Running tests for sub-modules

```bash
mvn -P it.kvstore verify
mvn -P it.kvclient verify
mvn -P it.kvquery verify
mvn -P it.kvdatacheck verify
mvn -P it.kvtif verify
```

> [!NOTE] It is possible to combine the test profiles.
> If you need to run both kvclient and kvquery tests, use:
> `mvn -P it.kvclient,it.kvquery verify`

## Help

There are a few ways to get help or report issues:

- Open an issue in the [Issues](./issues) page.
- Post your question on the [Oracle NoSQL Database Community](https://forums.oracle.com/ords/apexds/domain/dev-community/category/nosql_database?tags=nosql-database-discussions).

When requesting help please be sure to include as much detail as possible,
including version of the SDK and **simple**, standalone example code as needed.

## Changes

See the [Changelog](./CHANGELOG.html).

## Contributing

This project welcomes contributions from the community. Before submitting a
pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible
security vulnerability disclosure process

## License

Copyright (C) 2024, 2025 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the Apache License 2.0. See
[LICENSE](./LICENSE.txt) for details.
