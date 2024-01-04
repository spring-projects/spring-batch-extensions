# spring-batch-neo4j

This extension contains an `ItemReader` and `ItemWriter` implementations for [Neo4j](https://neo4j.com).

# Usage example

The `Neo4jItemReader` can be configured as follows:

```java
Neo4jItemReader<User> reader = new Neo4jItemReaderBuilder<User>()
        .neo4jTemplate(neo4jTemplate)
        .name("userReader")
        .statement(Cypher.match(userNode).returning(userNode))
        .targetType(User.class)
        .pageSize(50)
        .build();
```

The `Neo4jItemWriter` can be configured as follows:

```java
Neo4jItemWriter<User> writer = new Neo4jItemWriterBuilder<User>()
        .neo4jTemplate(neo4jTemplate)
        .neo4jDriver(driver)
        .neo4jMappingContext(mappingContext)
        .build();
```

## Minimal Spring Boot example

With a Spring Boot application containing the additional dependencies `spring-boot-starter-neo4j` and `spring-batch-neo4j`,
the following _build.gradle_ dependency definition is the minimal needed. 
Please note the exclusion for Spring JDBC from the `spring-boot-starter-batch` to avoid any need for JDBC-based connections.

```groovy
dependencies {
    implementation ('org.springframework.boot:spring-boot-starter-batch') {
        exclude group: 'org.springframework', module: 'spring-jdbc'
        exclude group: 'org.springframework.boot', module: 'spring-boot-starter-jdbc'
    }
    // current development version 0.2.0-SNAPSHOT
    implementation 'org.springframework.batch.extensions:spring-batch-neo4j'
    implementation 'org.springframework.boot:spring-boot-starter-data-neo4j'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.batch:spring-batch-test'
}
```

```java
@SpringBootApplication
public class TestSpringBatchApplication implements CommandLineRunner {
    // those dependencies are created by Spring Boot's
    // spring-data-neo4j autoconfiguration
    @Autowired
    private Driver driver;
    @Autowired
    private Neo4jMappingContext mappingContext;
    @Autowired
    private Neo4jTemplate neo4jTemplate;
  
    public static void main(String[] args) {
      SpringApplication.run(TestSpringBatchApplication.class, args);
    }
  
    @Override
    public void run(String... args) {
      // writing
      Neo4jItemWriter<User> writer = new Neo4jItemWriterBuilder<User>()
        .neo4jTemplate(neo4jTemplate)
        .neo4jDriver(driver)
        .neo4jMappingContext(mappingContext)
        .build();
      writer.write(Chunk.of(new User("id1", "ab"), new User("id2", "bb")));
      
      // reading
      org.neo4j.cypherdsl.core.Node userNode = Cypher.node("User");
      Neo4jItemReader<User> reader = new Neo4jItemReaderBuilder<User>()
        .neo4jTemplate(neo4jTemplate)
        .name("userReader")
        .statement(Cypher.match(userNode).returning(userNode))
        .targetType(User.class)
        .build();
      List<User> allUsers = new ArrayList<>();
      User user = null;
      while ((user = reader.read()) != null) {
        System.out.printf("Found user: %s%n", user.name);
        allUsers.add(user);
      }
      
      // deleting
      writer.setDelete(true);
      writer.write(Chunk.of(allUsers.toArray(new User[]{})));
    }
  
    @Node("User")
    public static class User {
      @Id public final String id;
      public final String name;
      
      public User(String id, String name) {
        this.id = id;
        this.name = name;
      }
    }
}
```