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
  - Download and install a [Java](https://www.oracle.com/java/technologies/downloads/)
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
  - kvquery-IT (QTF tests, depends on kvstore, kvstore-IT)
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

> [!NOTE]
> You can find the folder under `packaging/target/`. Above command will build
> and package all modules, but it will skip tests execution by default.

## Running Tests

### Running tests for all modules in async mode

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

> [!TIP]
> It is possible to combine the test profiles.
> If you need to run both kvclient and kvquery tests, use:
> `mvn -P it.kvclient,it.kvquery verify`

## Documentation

General documentation about the Oracle NoSQL Database and the Oracle NoSQL
Database Cloud Service can be found in these locations:

- [Oracle NoSQL Database Cloud Service](https://docs.oracle.com/en/cloud/paas/nosql-cloud/)
- [Oracle NoSQL Database On Premise](https://docs.oracle.com/en/database/other-databases/nosql-database/)

## Oracle NoSQL SDK

The Oracle NoSQL SDK drivers provide interfaces, documentation, and examples
to help develop applications that connect to the Oracle NoSQL Database Cloud
Service or Oracle NoSQL Database.

- [Oracle NoSQL SDK for Java](https://github.com/oracle/nosql-java-sdk)
- [Node.js for Oracle NoSQL Database](https://github.com/oracle/nosql-node-sdk)
- [.NET SDK for Oracle NoSQL Database](https://github.com/oracle/nosql-dotnet-sdk)
- [Oracle NoSQL Database Go SDK](https://github.com/oracle/nosql-go-sdk)
- [Oracle NoSQL Database SDK for Spring Data](https://github.com/oracle/nosql-spring-sdk)
- [Oracle NoSQL Database Python SDK](https://github.com/oracle/nosql-python-sdk)
- [Oracle NoSQL Database Rust SDK](https://github.com/oracle/nosql-rust-sdk)

## Oracle NoSQL Plugins

Oracle NoSQL Database Plugins enhances your experience of building an application:

- [Oracle NoSQL Database IntelliJ Plugin](https://github.com/oracle/nosql-intellij-plugin).
  The Intellij plugins is hosted in GitHub.
- [Oracle NoSQL Database Visual Studio Plugin](https://marketplace.visualstudio.com/items?itemName=Oracle.nosql-vscode).
  The Visual Studio Marketplace hosts a Visual Studios plugin for NoSQL

You can use Oracle NoSQL Database plugins to:

- View the tables in a well-defined tree structure with Table Explorer.
- View information on columns, indexes, primary key(s), and shard key(s) for a table.
- Create tables using form-based schema entry or supply DDL statements.
- Create Indexes.
- Execute SELECT SQL queries on a table and view query results in tabular format.
- Execute DML statements to update, insert, and delete data from a table.
- and much more

## Container image on GitHub repository

This [GitHub repository](https://github.com/oracle/docker-images/tree/main/NoSQL)
contains Dockerfiles, documentation and samples to build container images for
Oracle NoSQL.

### Highlights

1. This container image can be used by application developers who need to
   develop and unit test their Oracle NoSQL Database applications.
2. Behind the scene, we are using KVlite, and I added HTTP Proxy support
3. This container image was built on open-source NoSQL and published in GitHub
   Container Registry.
4. Users can clone this repository and build their image or pull the image
   directly from the GitHub Container Registry
5. There are 2 container images available, one using a secure configuration
   and one using a non-secure configuration

## Help

There are a few ways to get help or report issues:

- Open an issue in the [Issues](https://github.com/oracle/nosql/issues) page
  or start a [Discussion](https://github.com/oracle/nosql/discussions).
- Post your question on the [Oracle NoSQL Database Community](https://forums.oracle.com/ords/apexds/domain/dev-community/category/nosql_database?tags=nosql-database-discussions).
- Please send email to: [NoSQL help mailbox](mailto:nosql_mb@oracle.com)

When requesting help please be sure to include as much detail as possible,
including version of the SDK and **simple**, standalone example code as needed.

## Changes

See the [Changelog](./CHANGELOG.md).

## Contributing

This project welcomes contributions from the community. Before submitting a
pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible
security vulnerability disclosure process

## License

Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.

This software is licensed under the Apache License 2.0. See
[LICENSE](./LICENSE.txt) for details.
