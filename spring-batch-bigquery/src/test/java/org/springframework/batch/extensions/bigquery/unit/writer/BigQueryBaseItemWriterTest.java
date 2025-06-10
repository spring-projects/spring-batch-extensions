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

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryBaseItemWriter;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

class BigQueryBaseItemWriterTest extends AbstractBigQueryTest {

    private static final TableId TABLE_ID = TableId.of("dataset", "table");

    @Test
    void testGetTable() {
        Table expected = Mockito.mock(Table.class);
        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.getTable(TABLE_ID)).thenReturn(expected);

        TestWriter writer = new TestWriter();
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(WriteChannelConfiguration.of(TABLE_ID));

        Assertions.assertEquals(expected, writer.testGetTable());
    }

    @Test
    void testSetDatasetInfo() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());
        DatasetInfo expected = DatasetInfo.of("test");

        writer.setDatasetInfo(expected);

        DatasetInfo actual = (DatasetInfo) handle
                .findVarHandle(BigQueryBaseItemWriter.class, "datasetInfo", DatasetInfo.class)
                .get(writer);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testSetJobConsumer() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());
        Consumer<Job> expected = job -> {};

        writer.setJobConsumer(expected);

        Consumer<Job> actual = (Consumer<Job>) handle
                .findVarHandle(BigQueryBaseItemWriter.class, "jobConsumer", Consumer.class)
                .get(writer);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testSetWriteChannelConfig() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());
        WriteChannelConfiguration expected = WriteChannelConfiguration.newBuilder(TABLE_ID).build();

        writer.setWriteChannelConfig(expected);

        WriteChannelConfiguration actual = (WriteChannelConfiguration) handle
                .findVarHandle(BigQueryBaseItemWriter.class, "writeChannelConfig", WriteChannelConfiguration.class)
                .get(writer);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testSetBigQuery() throws IllegalAccessException, NoSuchFieldException {
        TestWriter writer = new TestWriter();
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());
        BigQuery expected = prepareMockedBigQuery();

        writer.setBigQuery(expected);

        BigQuery actual = (BigQuery) handle
                .findVarHandle(BigQueryBaseItemWriter.class, "bigQuery", BigQuery.class)
                .get(writer);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void testWrite() throws Exception {
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());
        AtomicBoolean consumerCalled = new AtomicBoolean();

        Job job = Mockito.mock(Job.class);
        Mockito.when(job.getJobId()).thenReturn(JobId.newBuilder().build());

        TableDataWriteChannel channel = Mockito.mock(TableDataWriteChannel.class);
        Mockito.when(channel.getJob()).thenReturn(job);

        BigQuery bigQuery = prepareMockedBigQuery();
        Mockito.when(bigQuery.writer(Mockito.any(WriteChannelConfiguration.class))).thenReturn(channel);

        TestWriter writer = new TestWriter();
        writer.setBigQuery(bigQuery);
        writer.setJobConsumer(j -> consumerCalled.set(true));
        writer.setWriteChannelConfig(WriteChannelConfiguration.of(TABLE_ID));

        writer.write(TestConstants.CHUNK);

        AtomicLong actual = (AtomicLong) handle
                .findVarHandle(BigQueryBaseItemWriter.class, "bigQueryWriteCounter", AtomicLong.class)
                .get(writer);

        Assertions.assertEquals(1L, actual.get());
        Assertions.assertTrue(consumerCalled.get());

        Mockito.verify(channel).write(Mockito.any(ByteBuffer.class));
        Mockito.verify(channel).close();
        Mockito.verify(channel, Mockito.times(2)).getJob();
        Mockito.verifyNoMoreInteractions(channel);
    }

    @Test
    void testBaseAfterPropertiesSet_Exception() {
        TestWriter writer = new TestWriter();
        WriteChannelConfiguration.Builder channelBuilder = WriteChannelConfiguration.newBuilder(TABLE_ID);

        // bigQuery
        IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("BigQuery service must be provided", actual.getMessage());

        // writeChannelConfig
        writer.setBigQuery(prepareMockedBigQuery());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Write channel configuration must be provided", actual.getMessage());

        // format
        writer.setWriteChannelConfig(channelBuilder.build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Data format must be provided", actual.getMessage());

        // bigtable
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.bigtable()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Google BigTable is not supported", actual.getMessage());

        // googleSheets
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.googleSheets()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Google Sheets is not supported", actual.getMessage());

        // datastore
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.datastoreBackup()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Google Datastore is not supported", actual.getMessage());

        // parquet
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.parquet()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Parquet is not supported", actual.getMessage());

        // orc
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.orc()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Orc is not supported", actual.getMessage());

        // avro
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.avro()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Avro is not supported", actual.getMessage());

        // iceberg
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.iceberg()).build());
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Iceberg is not supported", actual.getMessage());

        // dataset
        writer.setWriteChannelConfig(channelBuilder.setFormatOptions(FormatOptions.csv()).build());
        writer.setDatasetInfo(DatasetInfo.of("dataset-1"));
        actual = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
        Assertions.assertEquals("Dataset should be configured properly", actual.getMessage());
    }

    @Test
    void testBaseAfterPropertiesSet_Dataset() throws IllegalAccessException, NoSuchFieldException {
        MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryBaseItemWriter.class, MethodHandles.lookup());

        DatasetInfo datasetInfo = DatasetInfo.of(TABLE_ID.getDataset());
        BigQuery bigQuery = prepareMockedBigQuery();

        TestWriter writer = new TestWriter();
        writer.setBigQuery(bigQuery);
        writer.setWriteChannelConfig(WriteChannelConfiguration.newBuilder(TABLE_ID).setFormatOptions(FormatOptions.json()).build());

        writer.afterPropertiesSet();

        DatasetInfo actual = (DatasetInfo) handle.findVarHandle(BigQueryBaseItemWriter.class, "datasetInfo", DatasetInfo.class).get(writer);
        Assertions.assertEquals(datasetInfo, actual);

        Mockito.verify(bigQuery).create(datasetInfo);
        Mockito.verify(bigQuery).getDataset(TABLE_ID.getDataset());
        Mockito.verifyNoMoreInteractions(bigQuery);
    }

    @Test
    void testTableHasDefinedSchema() {
        TestWriter writer = new TestWriter();
        Table table = Mockito.mock(Table.class);

        // Null
        Assertions.assertFalse(writer.testTableHasDefinedSchema(null));

        // Without definition
        Assertions.assertFalse(writer.testTableHasDefinedSchema(table));

        // Without schema
        StandardTableDefinition.Builder definitionBuilder = StandardTableDefinition.newBuilder();
        Mockito.when(table.getDefinition()).thenReturn(definitionBuilder.build());
        Assertions.assertFalse(writer.testTableHasDefinedSchema(table));

        // With schema
        Mockito.when(table.getDefinition()).thenReturn(definitionBuilder.setSchema(Schema.of(Field.of(TestConstants.AGE, StandardSQLTypeName.STRING))).build());
        Assertions.assertTrue(writer.testTableHasDefinedSchema(table));
    }

    private static final class TestWriter extends BigQueryBaseItemWriter<PersonDto> {

        @Override
        protected void doInitializeProperties(List<? extends PersonDto> items) {}

        @Override
        protected List<byte[]> convertObjectsToByteArrays(List<? extends PersonDto> items) {
            return items.stream().map(Objects::toString).map(String::getBytes).toList();
        }

        @Override
        protected void performFormatSpecificChecks() {}

        public Table testGetTable() {
            return getTable();
        }

        public boolean testTableHasDefinedSchema(Table table) {
            return tableHasDefinedSchema(table);
        }
    }
}
