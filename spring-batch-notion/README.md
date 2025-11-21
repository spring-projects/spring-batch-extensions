# Spring Batch Notion [![Maven Central](https://img.shields.io/maven-central/v/org.springframework.batch.extensions/spring-batch-notion?label=Maven%20Central)](https://central.sonatype.com/artifact/org.springframework.batch.extensions/spring-batch-notion) [![javadoc](https://javadoc.io/badge2/org.springframework.batch.extensions/spring-batch-notion/javadoc.svg)](https://javadoc.io/doc/org.springframework.batch.extensions/spring-batch-notion)

[![Spring Batch Notion](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-notion.yml/badge.svg?branch=main)](https://github.com/spring-projects/spring-batch-extensions/actions/workflows/spring-batch-notion.yml?query=branch%3Amain)

This project provides a [Spring Batch][] extension module that adds support for [Notion][].

## Compatibility

Spring Batch Notion is based on Spring Batch 6 and tested on Spring Boot 4, thus requiring at least Java 17.

Compatibility is guaranteed only with the Spring Batch versions under [OSS support](https://spring.io/projects/spring-batch#support). 

## Getting Started

### Maven

```xml
<dependency>
  <groupId>org.springframework.batch.extensions</groupId>
  <artifactId>spring-batch-notion</artifactId>
  <version>${spring-batch-notion.version}</version>
</dependency>
```

### Gradle

```kotlin
implementation("org.springframework.batch.extensions:spring-batch-notion:${springBatchNotionVersion}")
```

## NotionDatabaseItemReader

The `NotionDatabaseItemReader` is a restartable `ItemReader` that reads entries from a [Notion Database] via a paging technique.

A minimal configuration of the item reader is as follows:

```java
NotionDatabaseItemReader<Item> itemReader() {
    String token = System.getenv("NOTION_TOKEN");
    String databaseId = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"; // UUID
    PropertyMapper<Item> propertyMapper = new CustomPropertyMapper();
    return new NotionDatabaseItemReader<>(token, databaseId, propertyMapper);
}
```

The following constructor parameters should be provided:

| Property         | Description                                                                                                               |
|------------------|---------------------------------------------------------------------------------------------------------------------------|
| `token`          | The Notion integration token.                                                                                             |
| `databaseId`     | UUID of the database to read from.                                                                                        |
| `propertyMapper` | The `PropertyMapper` responsible for mapping properties of a Notion item into a Java object.                              |

and the following configuration options are available:

| Property         | Required | Default                     | Description                                                                                                               |
|------------------|----------|-----------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `baseUrl`        | no       | `https://api.notion.com/v1` | Base URL of the Notion API. A custom value can be provided for testing purposes (e.g., the URL of a [WireMock][] server). |
| `filter`         | no       | `null`                      | `Filter` condition to limit the returned items.                                                                           |
| `pageSize`       | no       | `100`                       | Number of items to be read with each page. Must be greater than zero and less than or equal to 100.                       |
| `sorts`          | no       | `null`                      | `Sort` conditions to order the returned items. Each condition is applied following the declaration order.                 |

In addition to the Notion-specific configuration, all the configuration options of the Spring Batch
[`AbstractPaginatedDataItemReader`](https://docs.spring.io/spring-batch/docs/current/api/org/springframework/batch/item/data/AbstractPaginatedDataItemReader.html)
are supported.

### PropertyMapper

The `NotionDatabaseItemReader` requires a `PropertyMapper` to map the properties of a Notion item into an object.

Currently, only properties of type [Title](https://developers.notion.com/reference/property-object#title)
and [Rich Text](https://developers.notion.com/reference/property-object#rich-text) are supported,
and both are converted to strings.

The following `PropertyMapper` implementations are provided out of the box.

| Name                        | Description                                                                                                                                                                |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BeanWrapperPropertyMapper` | Supports JavaBeans. Requires a default constructor and expects the setter names to match the Notion item property names (case-insensitive).                                |
| `ConstructorPropertyMapper` | Supports types with a constructor with arguments. Requires the constructor to be unique and its argument names to match the Notion item property names (case-insensitive). |
| `RecordPropertyMapper`      | Supports Java records. It uses the canonical constructor and requires the component names to match the Notion item property names (case-insensitive).                      |

All implementations above offer two constructors:
* One accepting the `Class` instance of the type to be mapped
* One without parameters, for cases where the type to be mapped can be inferred by the generic type of the variable or method enclosing the constructor declaration

In case none of the provided implementations is suitable, a custom one can be provided.

## NotionDatabaseItemWriter

Currently not provided but will be added in the future.

## License

The Spring Batch Notion is released under version 2.0 of the [Apache License][].

[Apache License]: https://www.apache.org/licenses/LICENSE-2.0
[Notion]: https://notion.so/
[Notion Database]: https://www.notion.so/help/category/databases
[Spring Batch]: https://github.com/spring-projects/spring-batch
[WireMock]: https://wiremock.org/
