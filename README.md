Spring Batch Extensions
=============================

The Spring Batch Extensions project provides extension modules for [Spring Batch][]. This project is part of the [Spring organization][] on GitHub.

## Available Modules


## Samples

Under the `samples` directory, you will find samples for the various modules. Please refer to the documentation of each sample for further details.

## Getting support

Check out the [spring-batch][spring-batch tag] tag
on [Stack Overflow][]. [Commercial support][] is available too.

## Related GitHub projects

* [Spring Batch][]
* [Spring Batch Admin][]
* [Spring XD][]
* [Spring for Apache Hadoop][]
* [Spring Boot][]

## Issue Tracking

Report issues via the [Spring Integration Extensions JIRA][].

## Building from source

Each module of the *Spring Batch Extensions* project is hosted as independent project with its own release cycle. For the build process of individual modules we recomend using a [Maven][]-based build system modelled after the [Spring Batch][] project. 

Therefore, the following build instructions should generally apply for most, if not all, *Spring Batch Extensions*. In the instructions below, [`mvn`][] is invoked from the root of the source tree and serves as a cross-platform, self-contained bootstrap mechanism for the build. The only prerequisites are [Git][] and JDK 1.6+.

### Check out the sources

`git clone git://github.com/spring-projects/spring-batch-extensions.git`

### Go into the directory of a specific module

`cd module-name`

### Compile and test, build all jars

`mvn package`

### Install the modules jars into your local Maven cache

`mvn install`

## Import sources into your IDE

### Using Eclipse / STS

When using [SpringSource Tool Suite][] you can directly import Gradle based projects:

`File -> Import -> Maven Project`

Alternatively, you can also generate the Eclipse metadata (.classpath and .project files) using Maven:

`mvn eclipse`

Once complete, you may then import the projects into Eclipse as usual:

`File -> Import -> Existing projects into workspace`

### Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    mvn idea

## Contributing

[Pull requests][] are welcome. Please see the [contributor guidelines][] for details. Additionally, if you are contributing, we recommend following the process for Spring Batch as outlined in the [administrator guidelines][].

## Staying in touch

Follow the Spring Batch team members and contributors on Twitter:

* [@michaelminella](https://twitter.com/michaelminella) - Michael Minella
* [@chrisjs01](https://twitter.com/chrisjs01) - Chris Schaefer

## License

The Spring Batch Extensions Framework is released under version 2.0 of the [Apache License][] unless noted differently for individual extension Modules, but this should be the rare exception.

**We look forward to your contributions!!**

[Spring Batch]: https://github.com/spring-projects/spring-batch
[Spring organization]: https://github.com/spring-projects
[spring-batch tag]: http://stackoverflow.com/questions/tagged/spring-batch
[Stack Overflow]: http://stackoverflow.com/faq
[Commercial support]: asdf
[Spring Integration Extensions JIRA]: http://jira.springsource.org/browse/INTEXT
[the lifecycle of an issue]: https://github.com/cbeams/spring-framework/wiki/The-Lifecycle-of-an-Issue
[Gradle]: http://gradle.org
[`./gradlew`]: http://vimeo.com/34436402
[Git]: http://help.github.com/set-up-git-redirect
[Gradle build and release FAQ]: https://github.com/SpringSource/spring-framework/wiki/Gradle-build-and-release-FAQ
[Pull requests]: http://help.github.com/send-pull-requests
[contributor guidelines]: https://github.com/SpringSource/spring-integration/wiki/Contributor-guidelines
[administrator guidelines]: https://github.com/SpringSource/spring-integration/wiki/Administrator-Guidelines
[Spring Batch Admin]: https://github.com/spring-projects/spring-batch-admin
[Spring XD]: https://github.com/spring-projects/spring-xd
[Spring for Apache Hadoop]: https://github.com/spring-projects/spring-hadoop
[Spring Boot]: https://github.com/spring-projects/spring-boot
[Spring Tool Suite]: http://spring.io/tools/sts
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
