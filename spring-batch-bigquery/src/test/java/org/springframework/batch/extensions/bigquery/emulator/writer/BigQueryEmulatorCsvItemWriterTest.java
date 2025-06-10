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

package org.springframework.batch.extensions.bigquery.emulator.writer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.ResultVerifier;
import org.springframework.batch.extensions.bigquery.common.TableUtils;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.writer.base.BaseEmulatorItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;
import org.springframework.batch.item.Chunk;

class BigQueryEmulatorCsvItemWriterTest extends BaseEmulatorItemWriterTest {

    // TODO find out why data is not persisted into the sqlite database
    // at the same time it works fine with json/insertAll job/yaml file
    // cover 2 scenarios (predefined schema + generate on the fly)
    @Test
    @Disabled("Not working at the moment")
    void testWrite() throws Exception {
        TableId tableId = TableId.of(TestConstants.DATASET, TableUtils.generateTableName(TestConstants.CSV));
        Chunk<PersonDto> expectedChunk = Chunk.of(new PersonDto("Ivan", 30));

        WriteChannelConfiguration channelConfig = WriteChannelConfiguration
                .newBuilder(tableId)
                .setFormatOptions(FormatOptions.csv())
                .setSchema(PersonDto.getBigQuerySchema())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(channelConfig)
                .build();
        writer.afterPropertiesSet();

        writer.write(expectedChunk);

        ResultVerifier.verifyTableResult(expectedChunk, bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(5L)));
    }
}
