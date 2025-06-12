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
import org.springframework.batch.item.json.GsonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Stream;

class BigQueryJsonItemWriterTest extends AbstractBigQueryTest {

    private static final TableId TABLE_ID = TableId.of("1", "2");

    @Test
    void testSetMarshaller() throws IllegalAccessException, NoSuchFieldException {
        BigQueryJsonItemWriter<PersonDto> reader = new BigQueryJsonItemWriter<>();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryJsonItemWriter.class, MethodHandles.lookup());
        JsonObjectMarshaller<PersonDto> expected = new JacksonJsonObjectMarshaller<>();

        reader.setMarshaller(expected);

        JsonObjectMarshaller<PersonDto> actual = (JsonObjectMarshaller<PersonDto>) handle
                .findVarHandle(BigQueryJsonItemWriter.class, "marshaller", JsonObjectMarshaller.class)
                .get(reader);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testConvertObjectsToByteArrays() {
        TestWriter writer = new TestWriter();
        writer.setMarshaller(new JacksonJsonObjectMarshaller<>());

        // Empty
        Assertions.assertTrue(writer.testConvert(List.of()).isEmpty());

        // Not empty
        writer.setMarshaller(Record::toString);
        List<byte[]> actual = writer.testConvert(TestConstants.CHUNK.getItems());

        List<byte[]> expected = TestConstants.CHUNK
                .getItems()
                .stream()
                .map(PersonDto::toString)
                .map(s -> s.concat("\n"))
                .map(String::getBytes)
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
                .setSchema(Schema.of(Field.of(TestConstants.AGE, StandardSQLTypeName.STRING)))
                .build();
        Mockito.when(table.getDefinition()).thenReturn(tableDefinition);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);

        // marshaller
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
        Assertions.assertEquals("Marshaller is mandatory", actual.getMessage());

        // schema
        writer.setMarshaller(new JacksonJsonObjectMarshaller<>());
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(WriteChannelConfiguration.of(TABLE_ID, FormatOptions.csv()));
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
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
        writer.setMarshaller(new GsonJsonObjectMarshaller<>());

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

        public List<byte[]> testConvert(List<PersonDto> items) {
            return convertObjectsToByteArrays(items);
        }

        public void testPerformFormatSpecificChecks() {
            performFormatSpecificChecks();
        }
    }
}
