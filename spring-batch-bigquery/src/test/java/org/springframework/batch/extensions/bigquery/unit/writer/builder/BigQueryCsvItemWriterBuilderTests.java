/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.unit.writer.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;

class BigQueryCsvItemWriterBuilderTests extends AbstractBigQueryTest {

    private static final String TABLE = "persons_csv";

    private final Log logger = LogFactory.getLog(getClass());

    /**
     * Example how CSV writer is expected to be built without {@link org.springframework.context.annotation.Bean} annotation.
     */
    @Test
    void testCsvWriterWithRowMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        CsvMapper csvMapper = new CsvMapper();
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(TestConstants.DATASET).setLocation("europe-west-2").build();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(datasetInfo.getDatasetId().getDataset(), TABLE))
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .rowMapper(dto -> convertDtoToCsvByteArray(csvMapper, dto))
                .writeChannelConfig(writeConfiguration)
                .datasetInfo(datasetInfo)
                .jobConsumer(job -> this.logger.debug("Job with id: " + job.getJobId() + " is created"))
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    @Test
    void testCsvWriterWithCsvMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, TABLE))
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .writeChannelConfig(writeConfiguration)
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    private byte[] convertDtoToCsvByteArray(CsvMapper csvMapper, PersonDto dto) {
        try {
            return csvMapper.writerWithSchemaFor(PersonDto.class).writeValueAsBytes(dto);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
