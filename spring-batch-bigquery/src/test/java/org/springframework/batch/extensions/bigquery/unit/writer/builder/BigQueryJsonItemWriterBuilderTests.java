/*
 * Copyright 2002-2022 the original author or authors.
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryJsonItemWriterBuilder;

class BigQueryJsonItemWriterBuilderTests extends AbstractBigQueryTest {

    private final Log logger = LogFactory.getLog(getClass());

    /**
     * Example how JSON writer is expected to be built without {@link org.springframework.context.annotation.Bean} annotation.
     */
    @Test
    void testJsonWriterWithRowMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();
        ObjectMapper objectMapper = new ObjectMapper();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "persons_json"))
                .setFormatOptions(FormatOptions.json())
                .setSchema(Schema.of(
                        Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build()
                ))
                .build();

        BigQueryJsonItemWriter<PersonDto> writer = new BigQueryJsonItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .rowMapper(dto -> convertDtoToJsonByteArray(objectMapper, dto))
                .writeChannelConfig(writeConfiguration)
                .jobConsumer(job -> this.logger.debug("Job with id: " + job.getJobId() + " is created"))
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    @Test
    void testCsvWriterWithJsonMapper() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();

        WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, "persons_json"))
                .setAutodetect(true)
                .setFormatOptions(FormatOptions.json())
                .build();

        BigQueryJsonItemWriter<PersonDto> writer = new BigQueryJsonItemWriterBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .writeChannelConfig(writeConfiguration)
                .build();

        writer.afterPropertiesSet();

        Assertions.assertNotNull(writer);
    }

    private byte[] convertDtoToJsonByteArray(ObjectMapper objectMapper, PersonDto dto)  {
        try {
            return objectMapper.writeValueAsBytes(dto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
