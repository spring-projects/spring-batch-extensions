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

package org.springframework.batch.extensions.bigquery.unit.reader;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.core.convert.converter.Converter;

import java.lang.invoke.MethodHandles;
import java.util.List;

class BigQueryItemReaderTest extends AbstractBigQueryTest {

    @Test
    void testSetBigQuery() throws IllegalAccessException, NoSuchFieldException {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());
        BigQuery bigQuery = prepareMockedBigQuery();

        reader.setBigQuery(bigQuery);

        BigQuery actualBigQuery = (BigQuery) handle
                .findVarHandle(BigQueryQueryItemReader.class, "bigQuery", BigQuery.class)
                .get(reader);

        Assertions.assertEquals(bigQuery, actualBigQuery);
    }

    @Test
    void testSetRowMapper() throws IllegalAccessException, NoSuchFieldException {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());
        Converter<FieldValueList, PersonDto> rowMapper = source -> null;

        reader.setRowMapper(rowMapper);

        Converter<FieldValueList, PersonDto> actualRowMapper = (Converter<FieldValueList, PersonDto>) handle
                .findVarHandle(BigQueryQueryItemReader.class, "rowMapper", Converter.class)
                .get(reader);

        Assertions.assertEquals(rowMapper, actualRowMapper);
    }

    @Test
    void testSetJobConfiguration() throws IllegalAccessException, NoSuchFieldException {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryQueryItemReader.class, MethodHandles.lookup());
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration.newBuilder("select").build();

        reader.setJobConfiguration(jobConfiguration);

        QueryJobConfiguration actualJobConfiguration = (QueryJobConfiguration) handle
                .findVarHandle(BigQueryQueryItemReader.class, "jobConfiguration", QueryJobConfiguration.class)
                .get(reader);

        Assertions.assertEquals(jobConfiguration, actualJobConfiguration);
    }

    @Test
    void testRead() throws Exception {
        BigQuery bigQuery = prepareMockedBigQuery();
        List<PersonDto> items = TestConstants.CHUNK.getItems();

        Field name = Field.of(TestConstants.NAME, StandardSQLTypeName.STRING);
        Field age = Field.of(TestConstants.AGE, StandardSQLTypeName.INT64);

        PersonDto person1 = items.get(0);
        FieldValue value10 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.name());
        FieldValue value11 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.age().toString());

        FieldValueList row1 = FieldValueList.of(List.of(value10, value11), name, age);

        TableResult tableResult = Mockito.mock(TableResult.class);
        Mockito.when(tableResult.getValues()).thenReturn(List.of(row1));

        Mockito.when(bigQuery.query(Mockito.any(QueryJobConfiguration.class))).thenReturn(tableResult);

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();
        reader.setRowMapper(TestConstants.PERSON_MAPPER);
        reader.setBigQuery(bigQuery);
        reader.setJobConfiguration(QueryJobConfiguration.of("select"));

        // First call
        PersonDto actual = reader.read();
        Assertions.assertEquals(person1.name(), actual.name());
        Assertions.assertEquals(person1.age(), actual.age());

        // Second call
        Assertions.assertNull(reader.read());
    }

    @Test
    void testAfterPropertiesSet() {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReader<>();

        // bigQuery
        Assertions.assertThrows(IllegalArgumentException.class, reader::afterPropertiesSet);

        // rowMapper
        reader.setBigQuery(prepareMockedBigQuery());
        Assertions.assertThrows(IllegalArgumentException.class, reader::afterPropertiesSet);

        // jobConfiguration
        reader.setRowMapper(TestConstants.PERSON_MAPPER);
        Assertions.assertThrows(IllegalArgumentException.class, reader::afterPropertiesSet);

        // No exception
        reader.setJobConfiguration(QueryJobConfiguration.of("select"));
        Assertions.assertDoesNotThrow(reader::afterPropertiesSet);
    }
}
