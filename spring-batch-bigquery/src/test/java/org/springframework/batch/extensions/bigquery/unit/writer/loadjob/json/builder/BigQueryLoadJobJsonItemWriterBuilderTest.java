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

package org.springframework.batch.extensions.bigquery.unit.writer.loadjob.json.builder;

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
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.BigQueryLoadJobJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.builder.BigQueryLoadJobJsonItemWriterBuilder;
import org.springframework.batch.infrastructure.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.infrastructure.item.json.JsonObjectMarshaller;

import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;

class BigQueryLoadJobJsonItemWriterBuilderTest extends AbstractBigQueryTest {

	@Test
	void testBuild() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup jsonWriterHandle = MethodHandles.privateLookupIn(BigQueryLoadJobJsonItemWriter.class,
				MethodHandles.lookup());
		MethodHandles.Lookup baseWriterHandle = MethodHandles.privateLookupIn(BigQueryLoadJobBaseItemWriter.class,
				MethodHandles.lookup());

		JsonObjectMarshaller<PersonDto> marshaller = new JacksonJsonObjectMarshaller<>();
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(TestConstants.DATASET).setLocation("europe-west-2").build();
		Consumer<Job> jobConsumer = job -> {
		};
		BigQuery mockedBigQuery = prepareMockedBigQuery();

		WriteChannelConfiguration writeConfiguration = WriteChannelConfiguration
			.newBuilder(TableId.of(datasetInfo.getDatasetId().getDataset(), TestConstants.JSON))
			.setFormatOptions(FormatOptions.json())
			.build();

		BigQueryLoadJobJsonItemWriter<PersonDto> writer = new BigQueryLoadJobJsonItemWriterBuilder<PersonDto>()
			.marshaller(marshaller)
			.writeChannelConfig(writeConfiguration)
			.jobConsumer(jobConsumer)
			.bigQuery(mockedBigQuery)
			.datasetInfo(datasetInfo)
			.build();

		Assertions.assertNotNull(writer);

		JsonObjectMarshaller<PersonDto> actualMarshaller = (JsonObjectMarshaller<PersonDto>) jsonWriterHandle
			.findVarHandle(BigQueryLoadJobJsonItemWriter.class, "marshaller", JsonObjectMarshaller.class)
			.get(writer);

		WriteChannelConfiguration actualWriteChannelConfig = (WriteChannelConfiguration) jsonWriterHandle
			.findVarHandle(BigQueryLoadJobJsonItemWriter.class, "writeChannelConfig", WriteChannelConfiguration.class)
			.get(writer);

		Consumer<Job> actualJobConsumer = (Consumer<Job>) baseWriterHandle
			.findVarHandle(BigQueryLoadJobBaseItemWriter.class, "jobConsumer", Consumer.class)
			.get(writer);

		BigQuery actualBigQuery = (BigQuery) baseWriterHandle
			.findVarHandle(BigQueryLoadJobBaseItemWriter.class, "bigQuery", BigQuery.class)
			.get(writer);

		DatasetInfo actualDatasetInfo = (DatasetInfo) baseWriterHandle
			.findVarHandle(BigQueryLoadJobJsonItemWriter.class, "datasetInfo", DatasetInfo.class)
			.get(writer);

		Assertions.assertEquals(marshaller, actualMarshaller);
		Assertions.assertEquals(writeConfiguration, actualWriteChannelConfig);
		Assertions.assertEquals(jobConsumer, actualJobConsumer);
		Assertions.assertEquals(mockedBigQuery, actualBigQuery);
		Assertions.assertEquals(datasetInfo, actualDatasetInfo);
	}

}