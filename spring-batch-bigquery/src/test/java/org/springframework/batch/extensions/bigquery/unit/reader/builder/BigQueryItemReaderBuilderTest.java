/*
 * Copyright 2002-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.extensions.bigquery.unit.reader.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.core.convert.converter.Converter;

import java.lang.invoke.MethodHandles;
import java.util.stream.Stream;

class BigQueryItemReaderBuilderTest extends AbstractBigQueryTest {

    @Test
    void testBuild_WithoutJobConfiguration() throws IllegalAccessException, NoSuchFieldException {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        String query = "SELECT p.name, p.age FROM spring_batch_extensions.persons p LIMIT 1";
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .query(query)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        Assertions.assertNotNull(reader);

        BigQuery actualBigQuery = (BigQuery) handle
                .findVarHandle(BigQueryQueryItemReader.class, "bigQuery", BigQuery.class)
                .get(reader);

        Converter<FieldValueList, PersonDto> actualRowMapper = (Converter<FieldValueList, PersonDto>) handle
                .findVarHandle(BigQueryQueryItemReader.class, "rowMapper", Converter.class)
                .get(reader);

        QueryJobConfiguration actualJobConfiguration = (QueryJobConfiguration) handle
                .findVarHandle(BigQueryQueryItemReader.class, "jobConfiguration", QueryJobConfiguration.class)
                .get(reader);

        Assertions.assertEquals(mockedBigQuery, actualBigQuery);
        Assertions.assertEquals(TestConstants.PERSON_MAPPER, actualRowMapper);
        Assertions.assertEquals(QueryJobConfiguration.newBuilder(query).build(), actualJobConfiguration);
    }

    @Test
    void testBuild_WithoutRowMapper() throws IllegalAccessException, NoSuchFieldException {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());

        QueryJobConfiguration expectedJobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.persons p LIMIT 1")
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .jobConfiguration(expectedJobConfiguration)
                .targetType(PersonDto.class)
                .build();

        Assertions.assertNotNull(reader);

        BigQuery actualBigQuery = (BigQuery) handle
                .findVarHandle(BigQueryQueryItemReader.class, "bigQuery", BigQuery.class)
                .get(reader);

        Converter<FieldValueList, PersonDto> actualRowMapper = (Converter<FieldValueList, PersonDto>) handle
                .findVarHandle(BigQueryQueryItemReader.class, "rowMapper", Converter.class)
                .get(reader);

        QueryJobConfiguration actualJobConfiguration = (QueryJobConfiguration) handle
                .findVarHandle(BigQueryQueryItemReader.class, "jobConfiguration", QueryJobConfiguration.class)
                .get(reader);

        Assertions.assertEquals(mockedBigQuery, actualBigQuery);
        Assertions.assertNotNull(actualRowMapper);
        Assertions.assertEquals(expectedJobConfiguration, actualJobConfiguration);
    }

    @Test
    void testBuild() throws IllegalAccessException, NoSuchFieldException {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());

        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.csv p LIMIT 2")
                .setDestinationTable(TableId.of(TestConstants.DATASET, TestConstants.CSV))
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .jobConfiguration(jobConfiguration)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        Assertions.assertNotNull(reader);

        BigQuery actualBigQuery = (BigQuery) handle
                .findVarHandle(BigQueryQueryItemReader.class, "bigQuery", BigQuery.class)
                .get(reader);

        Converter<FieldValueList, PersonDto> actualRowMapper = (Converter<FieldValueList, PersonDto>) handle
                .findVarHandle(BigQueryQueryItemReader.class, "rowMapper", Converter.class)
                .get(reader);

        QueryJobConfiguration actualJobConfiguration = (QueryJobConfiguration) handle
                .findVarHandle(BigQueryQueryItemReader.class, "jobConfiguration", QueryJobConfiguration.class)
                .get(reader);

        Assertions.assertEquals(mockedBigQuery, actualBigQuery);
        Assertions.assertEquals(TestConstants.PERSON_MAPPER, actualRowMapper);
        Assertions.assertEquals(jobConfiguration, actualJobConfiguration);
    }

    @ParameterizedTest
    @MethodSource("brokenBuilders")
    void testBuild_Exception(String expectedMessage, BigQueryQueryItemReaderBuilder<?> builder) {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, builder::build);
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    private static Stream<Arguments> brokenBuilders() {
        final class HumanDto {}
        BigQuery bigQuery = Mockito.mock(BigQuery.class);
        return Stream.of(
                Arguments.of(
                        "No target type provided",
                        new BigQueryQueryItemReaderBuilder<PersonDto>().bigQuery(bigQuery)
                ),
                Arguments.of(
                        "Only Java record supported",
                        new BigQueryQueryItemReaderBuilder<HumanDto>().bigQuery(bigQuery).targetType(HumanDto.class)
                ),
                Arguments.of(
                        "No query provided",
                        new BigQueryQueryItemReaderBuilder<PersonDto>().bigQuery(bigQuery).rowMapper(source -> null)
                )
        );
    }
}
