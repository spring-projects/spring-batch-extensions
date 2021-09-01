# spring-batch-neo4j

This extension contains an `ItemReader` and `ItemWriter` implementations for [Neo4j](https://neo4j.com).

# Usage example

The `Neo4jItemReader` can be configured as follows:

```java
SessionFactory sessionFactory = ...
Neo4jItemReader<String> itemReader = new Neo4jItemReaderBuilder<String>()
        .sessionFactory(sessionFactory)
        .name("itemReader")
        .targetType(String.class)
        .startStatement("n=node(*)")
        .orderByStatement("n.age")
        .matchStatement("n -- m")
        .whereStatement("has(n.name)")
        .returnStatement("m")
        .pageSize(50)
        .build();
```

The `Neo4jItemWriter` can be configured as follows:

```java
SessionFactory sessionFactory = ...
Neo4jItemWriter<String> writer = new Neo4jItemWriterBuilder<String>()
        .sessionFactory(sessionFactory)
        .build();
```