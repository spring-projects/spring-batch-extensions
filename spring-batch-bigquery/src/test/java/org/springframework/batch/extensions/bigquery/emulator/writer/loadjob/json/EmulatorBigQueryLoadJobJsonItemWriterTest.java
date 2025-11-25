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

package org.springframework.batch.extensions.bigquery.emulator.writer.loadjob.json;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.extensions.bigquery.common.NameUtils;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.ResultVerifier;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.writer.base.EmulatorBaseItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.BigQueryLoadJobJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.builder.BigQueryLoadJobJsonItemWriterBuilder;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.json.JacksonJsonObjectMarshaller;

import java.util.stream.Stream;

class EmulatorBigQueryLoadJobJsonItemWriterTest extends EmulatorBaseItemWriterTest {

	@ParameterizedTest
	@MethodSource("tables")
	void testWrite(String table, boolean autodetect) throws Exception {
		TableId tableId = TableId.of(TestConstants.DATASET, table);
		Chunk<PersonDto> expectedChunk = Chunk.of(new PersonDto("Ivan", 30));

		WriteChannelConfiguration channelConfig = WriteChannelConfiguration.newBuilder(tableId)
			.setFormatOptions(FormatOptions.json())
			.setSchema(autodetect ? null : PersonDto.getBigQuerySchema())
			.setAutodetect(autodetect)
			.build();

		BigQueryLoadJobJsonItemWriter<PersonDto> writer = new BigQueryLoadJobJsonItemWriterBuilder<PersonDto>()
			.bigQuery(bigQuery)
			.writeChannelConfig(channelConfig)
			.marshaller(new JacksonJsonObjectMarshaller<>())
			.build();
		writer.afterPropertiesSet();

		writer.write(expectedChunk);

		ResultVerifier.verifyJavaRecordTableResult(expectedChunk,
				bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(5L)));
	}

	private static Stream<Arguments> tables() {
		return Stream.of(Arguments.of(NameUtils.generateTableName(TestConstants.JSON), false),

				// TODO auto detect is broken on big query container side?
				// Arguments.of(TableUtils.generateTableName(TestConstants.JSON), true),

				Arguments.of(TestConstants.JSON, false));
	}

}
