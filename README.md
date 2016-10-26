Spring Batch Extensions
=============================

The Spring Batch Extensions project provides extension modules for the [Spring Batch Project][]. This project is part of the [Spring organization][] on GitHub.

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

Report issues via the Spring Batch Extensions [GitHub Issue Tracker][].

## Building from source

Each module of the *Spring Batch Extensions* project is hosted as independent project with its own release cycle. For the build process of individual modules we recomend using a [Maven][] build system modelled after the [Spring Batch][] project. 

Therefore, the following build instructions should generally apply for most, if not all, *Spring Batch Extensions*. In the instructions below, `mvn` is invoked from the root of the source tree and serves as a cross-platform, self-contained bootstrap mechanism for the build. The only prerequisites are [Git][] and JDK 1.6+.

### Check out the sources

`git clone git://github.com/spring-projects/spring-batch-extensions.git`

### Go into the directory of a specific module

`cd spring-batch-extensions`

`cd module-name`

### Compile and test, build all jars

`mvn clean package`

### Install the modules jars into your local Maven cache

`mvn install`

## Import sources into your IDE

### Using Eclipse / STS

When using [Spring Tool Suite] you can directly import Maven based projects:

`File -> Import -> Maven Project`

Alternatively, you can also generate the Eclipse metadata (.classpath and .project files) using Maven:

`mvn eclipse:eclipse`

Once complete, you may then import the projects into Eclipse as usual:

`File -> Import -> Existing projects into workspace`

### Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    mvn idea:idea

## Contributing

[Pull requests][] are welcome. Please see the [Contributor Guidelines](CONTRIBUTING.md) for details. 

## Staying in touch

Follow the Spring Batch team members and contributors on Twitter:

* [@michaelminella](https://twitter.com/michaelminella) - Michael Minella
* [@chrisjs01](https://twitter.com/chrisjs01) - Chris Schaefer

## License

The Spring Batch Extensions Framework is released under version 2.0 of the [Apache License][] unless noted differently for individual extension Modules, but this should be the rare exception.

**We look forward to your contributions!!**

[Spring Batch]: https://github.com/spring-projects/spring-batch
[Spring Batch Project]: http://projects.spring.io/spring-batch/
[Spring organization]: https://github.com/spring-projects
[spring-batch tag]: http://stackoverflow.com/questions/tagged/spring-batch
[Stack Overflow]: http://stackoverflow.com/faq
[Commercial support]: https://www.vmware.com/support/services/vfabric-developer.html
[GitHub Issue Tracker]: https://github.com/spring-projects/spring-batch-extensions/issues
[Git]: http://help.github.com/set-up-git-redirect
[Pull requests]: http://help.github.com/send-pull-requests
[Spring Batch Admin]: https://github.com/spring-projects/spring-batch-admin
[Spring XD]: https://github.com/spring-projects/spring-xd
[Spring for Apache Hadoop]: https://github.com/spring-projects/spring-hadoop
[Spring Boot]: https://github.com/spring-projects/spring-boot
[Spring Tool Suite]: http://spring.io/tools/sts
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Maven]: http://maven.apache.org
[Contributor Guidelines]: com/spring-projects/spring-batch-extensions/blob/master/CONTRIBUTING.md
