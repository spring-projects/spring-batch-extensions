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

package org.springframework.batch.extensions.bigquery.unit.writer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.core.convert.converter.Converter;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Stream;

class BigQueryJsonItemWriterTest extends AbstractBigQueryTest {

    private static final TableId TABLE_ID = TableId.of("1", "2");

    @Test
    void testDoInitializeProperties() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        List<PersonDto> items = TestConstants.CHUNK.getItems();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryJsonItemWriter.class, MethodHandles.lookup());

        // Exception
        Assertions.assertThrows(IllegalStateException.class, () -> writer.testInitializeProperties(List.of()));

        // No exception
        writer.testInitializeProperties(items);
        Assertions.assertEquals(
                PersonDto.class.getSimpleName(),
                ((Class<PersonDto>) handle.findVarHandle(BigQueryJsonItemWriter.class, "itemClass", Class.class).get(writer)).getSimpleName()
        );
        ObjectWriter objectWriter = (ObjectWriter) handle.findVarHandle(BigQueryJsonItemWriter.class, "objectWriter", ObjectWriter.class).get(writer);
        Assertions.assertInstanceOf(JsonFactory.class, objectWriter.getFactory());
    }

    @Test
    void testSetRowMapper() throws IllegalAccessException, NoSuchFieldException {
        BigQueryJsonItemWriter<PersonDto> reader = new BigQueryJsonItemWriter<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryJsonItemWriter.class, MethodHandles.lookup());
        Converter<PersonDto, String> expected = source -> null;

        reader.setRowMapper(expected);

        Converter<PersonDto, String> actual = (Converter<PersonDto, String>) handle
                .findVarHandle(BigQueryJsonItemWriter.class, "rowMapper", Converter.class)
                .get(reader);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testConvertObjectsToByteArrays() {
        TestWriter writer = new TestWriter();

        // Empty
        Assertions.assertTrue(writer.testConvert(List.of()).isEmpty());

        // Not empty
        writer.setRowMapper(Record::toString);
        List<byte[]> actual = writer.testConvert(TestConstants.CHUNK.getItems());
        List<byte[]> expected = TestConstants.CHUNK.getItems().stream().map(PersonDto::toString).map(s -> s.concat("\n")).map(String::getBytes).toList();
        Assertions.assertEquals(expected.size(), actual.size());

        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertArrayEquals(expected.get(i), actual.get(i));
        }
    }

    @Test
    void testPerformFormatSpecificChecks() {
        TestWriter writer = new TestWriter();

        Table table = Mockito.mock(Table.class);
        StandardTableDefinition tableDefinition = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(Field.of(TestConstants.AGE, StandardSQLTypeName.STRING)))
                .build();
        Mockito.when(table.getDefinition()).thenReturn(tableDefinition);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);

        // schema
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(WriteChannelConfiguration.of(TABLE_ID, FormatOptions.csv()));
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Schema must be provided", actual.getMessage());

        // schema equality
        WriteChannelConfiguration channelConfig = WriteChannelConfiguration
                .newBuilder(TABLE_ID)
                .setSchema(Schema.of(Field.of(TestConstants.NAME, StandardSQLTypeName.STRING)))
                .setFormatOptions(FormatOptions.json())
                .build();
        writer.setWriteChannelConfig(channelConfig);
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Schema must be the same", actual.getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidFormats")
    void testPerformFormatSpecificChecks_Format(FormatOptions formatOptions) {
        Table table = Mockito.mock(Table.class);
        StandardTableDefinition tableDefinition = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(Field.of(TestConstants.AGE, StandardSQLTypeName.STRING)))
                .build();
        Mockito.when(table.getDefinition()).thenReturn(tableDefinition);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);

        TestWriter writer = new TestWriter();
        writer.setBigQuery(bigQuery);

        writer.setWriteChannelConfig(WriteChannelConfiguration.newBuilder(TABLE_ID).setAutodetect(true).setFormatOptions(formatOptions).build());
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Only %s format is allowed".formatted(FormatOptions.json().getType()), actual.getMessage());
    }

    static Stream<FormatOptions> invalidFormats() {
        return Stream.of(
                FormatOptions.parquet(),
                FormatOptions.avro(),
                FormatOptions.bigtable(),
                FormatOptions.datastoreBackup(),
                FormatOptions.googleSheets(),
                FormatOptions.iceberg(),
                FormatOptions.orc(),
                FormatOptions.csv()
        );
    }

    private static final class TestWriter extends BigQueryJsonItemWriter<PersonDto> {
        public void testInitializeProperties(List<PersonDto> items) {
            doInitializeProperties(items);
        }

        public List<byte[]> testConvert(List<PersonDto> items) {
            return convertObjectsToByteArrays(items);
        }

        public void testPerformFormatSpecificChecks() {
            performFormatSpecificChecks();
        }
    }
}
