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

package org.springframework.batch.extensions.bigquery.unit.writer.loadjob.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvFactory;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
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
import org.springframework.batch.extensions.bigquery.writer.loadjob.csv.BigQueryLoadJobCsvItemWriter;
import org.springframework.core.convert.converter.Converter;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Stream;

class BigQueryLoadJobCsvItemWriterTest extends AbstractBigQueryTest {

    private static final TableId TABLE_ID = TableId.of(TestConstants.DATASET, TestConstants.CSV);
    private static final Schema SCHEMA = Schema.of(Field.of(TestConstants.AGE, StandardSQLTypeName.STRING));

    @Test
    void testDoInitializeProperties() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        List<PersonDto> items = TestConstants.CHUNK.getItems();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryLoadJobCsvItemWriter.class, MethodHandles.lookup());

        // Exception
        Assertions.assertThrows(IllegalStateException.class, () -> writer.testInitializeProperties(List.of()));

        // No exception
        writer.testInitializeProperties(items);
        Assertions.assertEquals(
                PersonDto.class.getSimpleName(),
                ((Class<PersonDto>) handle.findVarHandle(BigQueryLoadJobCsvItemWriter.class, "itemClass", Class.class).get(writer)).getSimpleName()
        );
        ObjectWriter objectWriter = (ObjectWriter) handle
                .findVarHandle(BigQueryLoadJobCsvItemWriter.class, "objectWriter", ObjectWriter.class)
                .get(writer);
        Assertions.assertInstanceOf(CsvFactory.class, objectWriter.getFactory());
    }

    @Test
    void testSetRowMapper() throws IllegalAccessException, NoSuchFieldException {
        BigQueryLoadJobCsvItemWriter<PersonDto> reader = new BigQueryLoadJobCsvItemWriter<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryLoadJobCsvItemWriter.class, MethodHandles.lookup());
        Converter<PersonDto, byte[]> expected = source -> null;

        reader.setRowMapper(expected);

        Converter<PersonDto, byte[]> actual = (Converter<PersonDto, byte[]>) handle
                .findVarHandle(BigQueryLoadJobCsvItemWriter.class, "rowMapper", Converter.class)
                .get(reader);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testConvertObjectsToByteArrays() {
        TestWriter writer = new TestWriter();
        List<PersonDto> items = TestConstants.CHUNK.getItems();

        // Empty
        Assertions.assertTrue(writer.testConvert(List.of()).isEmpty());

        // Not empty (row mapper)
        writer.setRowMapper(source -> source.toString().getBytes());
        List<byte[]> actual = writer.testConvert(items);
        List<byte[]> expected = items.stream().map(PersonDto::toString).map(String::getBytes).toList();
        Assertions.assertEquals(expected.size(), actual.size());

        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertArrayEquals(expected.get(i), actual.get(i));
        }

        // Not empty (object writer)
        ObjectWriter csvWriter = new CsvMapper().writerWithTypedSchemaFor(PersonDto.class);
        writer.setRowMapper(null);
        writer.testInitializeProperties(items);
        actual = writer.testConvert(items);

        expected = items
                .stream()
                .map(pd -> {
                    try {
                        return csvWriter.writeValueAsBytes(pd);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

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
                .setSchema(SCHEMA)
                .build();
        Mockito.when(table.getDefinition()).thenReturn(tableDefinition);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);

        // schema
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(WriteChannelConfiguration.of(TABLE_ID, FormatOptions.json()));
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Schema must be provided", actual.getMessage());

        // schema equality
        WriteChannelConfiguration channelConfig = WriteChannelConfiguration
                .newBuilder(TABLE_ID)
                .setSchema(Schema.of(Field.of(TestConstants.NAME, StandardSQLTypeName.STRING)))
                .setFormatOptions(FormatOptions.csv())
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
                .setSchema(SCHEMA)
                .build();
        Mockito.when(table.getDefinition()).thenReturn(tableDefinition);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);

        TestWriter writer = new TestWriter();
        writer.setBigQuery(bigQuery);

        writer.setWriteChannelConfig(WriteChannelConfiguration.newBuilder(TABLE_ID).setAutodetect(true).setFormatOptions(formatOptions).build());
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Only %s format is allowed".formatted(FormatOptions.csv().getType()), actual.getMessage());
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
                FormatOptions.json()
        );
    }

    private static final class TestWriter extends BigQueryLoadJobCsvItemWriter<PersonDto> {
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
