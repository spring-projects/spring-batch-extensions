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

package org.springframework.batch.extensions.bigquery.gcloud.writer.loadjob.json;

import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.NameUtils;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.gcloud.writer.GcloudBaseBigQueryItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.BigQueryLoadJobJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.builder.BigQueryLoadJobJsonItemWriterBuilder;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class GcloudBigQueryLoadJobJsonItemWriterTest extends GcloudBaseBigQueryItemWriterTest {

	private static final TableId TABLE_ID = TableId.of(TestConstants.DATASET, TestConstants.JSON);

	@BeforeAll
	static void prepareTest() {
		if (BIG_QUERY.getDataset(TestConstants.DATASET) == null) {
			BIG_QUERY.create(DatasetInfo.of(TestConstants.DATASET));
		}

		if (BIG_QUERY.getTable(TestConstants.DATASET, TestConstants.JSON) == null) {
			TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
			BIG_QUERY.create(TableInfo.of(TABLE_ID, tableDefinition));
		}
	}

	@AfterAll
	static void cleanup() {
		BIG_QUERY.delete(TABLE_ID);
	}

	@ParameterizedTest
	@MethodSource("tables")
	void testWrite(String tableName, boolean autodetect) throws Exception {
		AtomicReference<Job> job = new AtomicReference<>();

		WriteChannelConfiguration channelConfiguration = WriteChannelConfiguration
			.newBuilder(TableId.of(TestConstants.DATASET, tableName))
			.setSchema(autodetect ? null : PersonDto.getBigQuerySchema())
			.setAutodetect(autodetect)
			.setFormatOptions(FormatOptions.json())
			.build();

		BigQueryLoadJobJsonItemWriter<PersonDto> writer = new BigQueryLoadJobJsonItemWriterBuilder<PersonDto>()
			.bigQuery(BIG_QUERY)
			.writeChannelConfig(channelConfiguration)
			.jobConsumer(job::set)
			.build();

		writer.afterPropertiesSet();
		writer.write(TestConstants.CHUNK);
		job.get().waitFor();

		verifyResults(tableName);
	}

	private static Stream<Arguments> tables() {
		return Stream.of(Arguments.of(NameUtils.generateTableName(TestConstants.JSON), false),
				Arguments.of(NameUtils.generateTableName(TestConstants.JSON), true),
				Arguments.of(TestConstants.JSON, false));
	}

}