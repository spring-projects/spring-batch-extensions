/*
 * Copyright 2002-2024 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.gcloud.writer;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.*;
import org.springframework.batch.extensions.bigquery.common.BigQueryDataLoader;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;

import java.util.concurrent.atomic.AtomicReference;

class BigQueryGcloudCsvItemWriterTest extends BaseBigQueryGcloudItemWriterTest {

    @BeforeAll
    static void prepareTest() {
        if (BIG_QUERY.getDataset(TestConstants.DATASET) == null) {
            BIG_QUERY.create(DatasetInfo.of(TestConstants.DATASET));
        }

        if (BIG_QUERY.getTable(TestConstants.DATASET, TestConstants.CSV) == null) {
            TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
            BIG_QUERY.create(TableInfo.of(TableId.of(TestConstants.DATASET, TestConstants.CSV), tableDefinition));
        }
    }

    @AfterAll
    static void cleanup() {
        BIG_QUERY.delete(TableId.of(TestConstants.DATASET, TestConstants.CSV));
    }

    @Test
    void testWriteCsv() throws Exception {
        AtomicReference<Job> job = new AtomicReference<>();

        WriteChannelConfiguration channelConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, TestConstants.CSV))
                .setSchema(PersonDto.getBigQuerySchema())
                .setAutodetect(false)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(BIG_QUERY)
                .writeChannelConfig(channelConfiguration)
                .jobConsumer(job::set)
                .build();

        writer.afterPropertiesSet();
        writer.write(BigQueryDataLoader.CHUNK);
        job.get().waitFor();

        verifyResults(TestConstants.CSV);
    }

}