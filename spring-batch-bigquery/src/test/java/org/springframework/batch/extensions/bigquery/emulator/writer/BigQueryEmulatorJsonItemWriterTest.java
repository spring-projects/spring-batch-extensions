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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.ResultVerifier;
import org.springframework.batch.extensions.bigquery.common.TableUtils;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.writer.base.BaseEmulatorItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryJsonItemWriterBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;

import java.util.stream.Stream;

class BigQueryEmulatorJsonItemWriterTest extends BaseEmulatorItemWriterTest {

    @ParameterizedTest
    @MethodSource("tables")
    void testWrite(String table, boolean autodetect) throws Exception {
        TableId tableId = TableId.of(TestConstants.DATASET, table);
        Chunk<PersonDto> expectedChunk = Chunk.of(new PersonDto("Ivan", 30));

        WriteChannelConfiguration channelConfig = WriteChannelConfiguration
                .newBuilder(tableId)
                .setFormatOptions(FormatOptions.json())
                .setSchema(autodetect ? null : PersonDto.getBigQuerySchema())
                .setAutodetect(autodetect)
                .build();

        BigQueryJsonItemWriter<PersonDto> writer = new BigQueryJsonItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(channelConfig)
                .marshaller(new JacksonJsonObjectMarshaller<>())
                .build();
        writer.afterPropertiesSet();

        writer.write(expectedChunk);

        ResultVerifier.verifyTableResult(expectedChunk, bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(5L)));
    }

    private static Stream<Arguments> tables() {
        return Stream.of(
                Arguments.of(TableUtils.generateTableName(TestConstants.JSON), false),

                // TODO auto detect is broken on big query contained side?
                // Arguments.of(TableUtils.generateTableName(TestConstants.JSON), true),

                Arguments.of(TestConstants.JSON, false)
        );
    }
}
