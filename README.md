Spring Batch Extensions
=============================

The Spring Batch Extensions project provides extension modules for the [Spring Batch Project][].
This project is part of the [Spring organization][] on GitHub.

## Available Modules

| Module | Description | Lead | Version | CI build |
| -------|-------------| -----| ------- |----------|
| spring-batch-excel | Support for [Microsoft Excel] | [@mdeinum](https://github.com/mdeinum) | 0.1.1 | [![Spring Batch Excel](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-excel.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-excel.yml) |
| spring-batch-elasticsearch | Support for [Elasticsearch] | TBA | 0.1.0-SNAPSHOT | [![Spring Batch Elasticsearch](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-elasticsearch.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-elasticsearch.yml) |
| spring-batch-bigquery | Support for [Google BigQuery] | [@dgray16](https://github.com/dgray16) | 0.1.0 | [![Spring Batch BigQuery](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-bigquery.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-bigquery.yml) |
| spring-batch-neo4j | Support for [Neo4j] | [@michael-simons](https://github.com/michael-simons) | 0.1.0 | [![Spring Batch Neo4j](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-neo4j.yml/badge.svg)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-neo4j.yml) |

## Getting support

Check out the [spring-batch][spring-batch tag] tag on Stack Overflow.

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
* [@b_e_n_a_s](https://twitter.com/b_e_n_a_s) - Mahmoud Ben Hassine
* [@mdeinum](https://twitter.com/mdeinum) - Marten Deinum
* [@rotnroll666](https://twitter.com/rotnroll666) - Michael Simons
* [@meistermeier](https://twitter.com/meistermeier) - Gerrit Meier

## License

The Spring Batch Extensions are released under version 2.0 of the [Apache License][] unless
noted differently for individual extension Modules, but this should be the rare exception.

**We look forward to your contributions!!**

[Spring Batch Project]: https://projects.spring.io/spring-batch/
[Spring organization]: https://github.com/spring-projects
[Microsoft Excel]: https://www.microsoft.com/en-us/microsoft-365/excel
[Elasticsearch]: https://www.elastic.co
[Google BigQuery]: https://cloud.google.com/bigquery
[Neo4j]: https://neo4j.com
[spring-batch tag]: https://stackoverflow.com/questions/tagged/spring-batch
[Spring Batch]: https://github.com/spring-projects/spring-batch
[Spring Boot]: https://github.com/spring-projects/spring-boot
[Spring Cloud Task]: https://github.com/spring-cloud/spring-cloud-task
[Spring Cloud Data Flow]: https://github.com/spring-cloud/spring-cloud-dataflow
[GitHub Issue Tracker]: https://github.com/spring-projects/spring-batch-extensions/issues
[Maven]: https://maven.apache.org
[Git]: https://help.github.com/set-up-git-redirect
[Spring Tool Suite]: https://spring.io/tools
[Intellij IDEA]: https://www.jetbrains.com/idea/
[Pull requests]: https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests
[Contributor Guidelines]: CONTRIBUTING.md
[Apache License]: https://www.apache.org/licenses/LICENSE-2.0
