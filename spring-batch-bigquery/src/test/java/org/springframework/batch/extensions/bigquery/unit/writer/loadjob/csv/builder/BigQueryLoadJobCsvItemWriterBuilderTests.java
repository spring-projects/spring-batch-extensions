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

package org.springframework.batch.extensions.bigquery.unit.writer.loadjob.csv.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.loadjob.BigQueryLoadJobBaseItemWriter;
import org.springframework.batch.extensions.bigquery.writer.loadjob.csv.BigQueryLoadJobCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.loadjob.csv.builder.BigQueryCsvItemWriterBuilder;
import org.springframework.core.convert.converter.Converter;

import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;

class BigQueryLoadJobCsvItemWriterBuilderTests extends AbstractBigQueryTest {

    @Test
    void testBuild() throws IllegalAccessException, NoSuchFieldException {
        MethodHandles.Lookup csvWriterHandle = MethodHandles.privateLookupIn(BigQueryLoadJobCsvItemWriter.class, MethodHandles.lookup());
        MethodHandles.Lookup baseWriterHandle = MethodHandles.privateLookupIn(BigQueryLoadJobBaseItemWriter.class, MethodHandles.lookup());

        Converter<PersonDto, byte[]> rowMapper = source -> new byte[0];
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(TestConstants.DATASET).setLocation("europe-west-2").build();
        Consumer<Job> jobConsumer = job -> {};
        BigQuery mockedBigQuery = prepareMockedBigQuery();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(datasetInfo.getDatasetId().getDataset(), TestConstants.CSV))
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryLoadJobCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .rowMapper(rowMapper)
                .writeChannelConfig(writeConfiguration)
                .jobConsumer(jobConsumer)
                .bigQuery(mockedBigQuery)
                .datasetInfo(datasetInfo)
                .build();

        Assertions.assertNotNull(writer);

        Converter<PersonDto, byte[]> actualRowMapper = (Converter<PersonDto, byte[]>) csvWriterHandle
                .findVarHandle(BigQueryLoadJobCsvItemWriter.class, "rowMapper", Converter.class)
                .get(writer);

        WriteChannelConfiguration actualWriteChannelConfig = (WriteChannelConfiguration) csvWriterHandle
                .findVarHandle(BigQueryLoadJobCsvItemWriter.class, "writeChannelConfig", WriteChannelConfiguration.class)
                .get(writer);

        Consumer<Job> actualJobConsumer = (Consumer<Job>) baseWriterHandle
                .findVarHandle(BigQueryLoadJobBaseItemWriter.class, "jobConsumer", Consumer.class)
                .get(writer);

        BigQuery actualBigQuery = (BigQuery) baseWriterHandle
                .findVarHandle(BigQueryLoadJobBaseItemWriter.class, "bigQuery", BigQuery.class)
                .get(writer);

        DatasetInfo actualDatasetInfo = (DatasetInfo) baseWriterHandle
                .findVarHandle(BigQueryLoadJobCsvItemWriter.class, "datasetInfo", DatasetInfo.class)
                .get(writer);

        Assertions.assertEquals(rowMapper, actualRowMapper);
        Assertions.assertEquals(writeConfiguration, actualWriteChannelConfig);
        Assertions.assertEquals(jobConsumer, actualJobConsumer);
        Assertions.assertEquals(mockedBigQuery, actualBigQuery);
        Assertions.assertEquals(datasetInfo, actualDatasetInfo);
    }

}