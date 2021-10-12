# spring-batch-kotlin-dsl

Spring batch extension for kotlin-dsl.

Design philosophy.

- Follow the origin dsl model.
- Prevent odd behavior which the origin dsl allowed.

## Requirements

- JDK 1.8 or higher
- Kotlin 1.5.x or higher
- Spring Batch 4.3.x

## Import

### Gradle

```kotlin
implementation("org.springframework.batch.extensions:spring-batch-kotlin-dsl:0.1.0")
```

### Maven

```xml

<dependency>
    <groupId>org.springframework.batch.extensions</groupId>
    <artifactId>spring-batch-kotlin-dsl</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Usage

### Register as a Bean

By annotation.

```kotlin
@EnableBatchProcessing // need this
@EnableBatchDsl
class BatchConfig
```

Or register it directly.

```kotlin
@Bean
fun batchDsl(
        beanFactory: BeanFactory,
        jobBuilderFactory: JobBuilderFactory,
        stepBuilderFactory: StepBuilderFactory
): BatchDsl = BatchDsl(
                beanFactory,
                jobBuilderFactory,
                stepBuilderFactory
        )
```
