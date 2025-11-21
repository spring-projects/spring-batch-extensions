Spring Batch Extensions
=============================

The Spring Batch Extensions project provides extension modules for the [Spring Batch Project][].
This project is part of the [Spring organization][] on GitHub.

## Available Modules

| Module                                                     | Description                   | Lead                                                 | Version                                                                                                                                                                                                                                 | CI build                                                                                                                                                                                                                                                                                |
|------------------------------------------------------------|-------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`spring-batch-bigquery`](spring-batch-bigquery)           | Support for [Google BigQuery] | [@dgray16](https://github.com/dgray16)               | [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-bigquery?label)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-bigquery)           | [![Spring Batch BigQuery](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-bigquery.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-bigquery.yml?query=branch%3Amain)                |
| [`spring-batch-elasticsearch`](spring-batch-elasticsearch) | Support for [Elasticsearch]   | TBA                                                  | [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-elasticsearch?label)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-elasticsearch) | [![Spring Batch Elasticsearch](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-elasticsearch.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-elasticsearch.yml?query=branch%3Amain) |
| [`spring-batch-excel`](spring-batch-excel)                 | Support for [Microsoft Excel] | [@mdeinum](https://github.com/mdeinum)               | [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-excel?label)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-excel)                 | [![Spring Batch Excel](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-excel.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-excel.yml?query=branch%3Amain)                         |
| [`spring-batch-neo4j`](spring-batch-neo4j)                 | Support for [Neo4j]           | [@michael-simons](https://github.com/michael-simons) | [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-neo4j?label)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-neo4j)                 | [![Spring Batch Neo4j](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-neo4j.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-neo4j.yml?query=branch%3Amain)                         |
| [`spring-batch-notion`](spring-batch-notion)               | Support for [Notion]          | [@scordio](https://github.com/scordio)               | [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-notion?label)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-notion)               | [![Spring Batch Notion](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-notion.yml/badge.svg?branch=main)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-notion.yml?query=branch%3Amain)          |

## Getting support

Check out the [`spring-batch`][spring-batch tag] tag on Stack Overflow.

## Related GitHub projects

* [Spring Batch][]
* [Spring Boot][]
* [Spring Cloud Task][]
* [Spring Cloud Data Flow][]

## Issue Tracking

Report issues via the Spring Batch Extensions [GitHub Issue Tracker][].

## Building from source

Each module of the *Spring Batch Extensions* project is hosted as an independent project with its own release cycle.
All modules are built with [Maven][]. The only prerequisites are [Git][] and JDK 1.8+.

### Check out the sources

`git clone git://github.com/spring-projects/spring-batch-extensions.git`

### Go into the directory of a specific module

`cd spring-batch-extensions/module-name`

### Compile and test, build all jars

`mvn clean package`

### Install the modules jars into your local Maven repository

`mvn install`

## Import sources into your IDE

### Using Eclipse / STS

When using [Spring Tool Suite] you can directly import Maven based projects:

`File -> Import -> Maven Project`

Alternatively, you can generate the Eclipse metadata (.classpath and .project files) using Maven:

`mvn eclipse:eclipse`

Once complete, you may then import the projects into Eclipse as usual:

`File -> Import -> Existing projects into workspace`

### Using IntelliJ IDEA

When using [Intellij IDEA] you can directly import Maven based projects:

`File -> Open` then select the directory of the module.

Alternatively, you can generate the Intellij IDEA metadata (.iml and .ipr files) using Maven:

`mvn idea:idea`

Once complete, you may then import the projects into Intellij IDEA as usual.

## Contributing

[Pull requests][] are welcome. Please see the [Contributor Guidelines][] for details. 

## Staying in touch

Follow the Spring Batch team members and contributors on Twitter:

* [@michaelminella](https://twitter.com/michaelminella) - Michael Minella
* [@fmbenhassine](https://twitter.com/fmbenhassine) - Mahmoud Ben Hassine
* [@mdeinum](https://twitter.com/mdeinum) - Marten Deinum
* [@rotnroll666](https://twitter.com/rotnroll666) - Michael Simons
* [@meistermeier](https://twitter.com/meistermeier) - Gerrit Meier
* [@stefanocodes](https://twitter.com/stefanocodes) - Stefano Cordio

## License

The Spring Batch Extensions are released under version 2.0 of the [Apache License][] unless
noted differently for individual extension Modules, but this should be the rare exception.

**We look forward to your contributions!!**

[Apache Geode]: https://geode.apache.org
[Apache License]: https://www.apache.org/licenses/LICENSE-2.0
[Contributor Guidelines]: CONTRIBUTING.md
[Elasticsearch]: https://www.elastic.co
[Git]: https://help.github.com/set-up-git-redirect
[GitHub Issue Tracker]: https://github.com/spring-projects/spring-batch-extensions/issues
[Google BigQuery]: https://cloud.google.com/bigquery
[Intellij IDEA]: https://www.jetbrains.com/idea/
[Maven]: https://maven.apache.org
[Microsoft Excel]: https://www.microsoft.com/en-us/microsoft-365/excel
[Neo4j]: https://neo4j.com
[Notion]: https://notion.so/
[Pull requests]: https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests
[Spring Batch]: https://github.com/spring-projects/spring-batch
[Spring Batch Project]: https://projects.spring.io/spring-batch/
[Spring Boot]: https://github.com/spring-projects/spring-boot
[Spring Cloud Data Flow]: https://github.com/spring-cloud/spring-cloud-dataflow
[Spring Cloud Task]: https://github.com/spring-cloud/spring-cloud-task
[Spring organization]: https://github.com/spring-projects
[Spring Tool Suite]: https://spring.io/tools
[spring-batch tag]: https://stackoverflow.com/questions/tagged/spring-batch
