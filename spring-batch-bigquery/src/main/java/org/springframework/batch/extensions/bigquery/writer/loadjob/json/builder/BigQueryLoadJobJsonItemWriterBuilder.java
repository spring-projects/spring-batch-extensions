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

package org.springframework.batch.extensions.bigquery.writer.loadjob.json.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.springframework.batch.extensions.bigquery.writer.loadjob.json.BigQueryLoadJobJsonItemWriter;
import org.springframework.batch.infrastructure.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.infrastructure.item.json.JsonObjectMarshaller;

import java.util.function.Consumer;

/**
 * A builder for {@link BigQueryLoadJobJsonItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href=
 * "https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/loadjob/json/builder/BigQueryLoadJobJsonItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryLoadJobJsonItemWriterBuilder<T> {

	private JsonObjectMarshaller<T> marshaller;

	private Consumer<Job> jobConsumer;

	private DatasetInfo datasetInfo;

	private WriteChannelConfiguration writeChannelConfig;

	private BigQuery bigQuery;

	/**
	 * Default constructor
	 */
	public BigQueryLoadJobJsonItemWriterBuilder() {
	}

	/**
	 * Converts your DTO into a {@link String}.
	 * @param marshaller your mapper
	 * @return {@link BigQueryLoadJobJsonItemWriterBuilder}
	 * @see BigQueryLoadJobJsonItemWriter#setMarshaller(JsonObjectMarshaller)
	 */
	public BigQueryLoadJobJsonItemWriterBuilder<T> marshaller(final JsonObjectMarshaller<T> marshaller) {
		this.marshaller = marshaller;
		return this;
	}

	/**
	 * Provides additional information about the
	 * {@link com.google.cloud.bigquery.Dataset}.
	 * @param datasetInfo BigQuery dataset info
	 * @return {@link BigQueryLoadJobJsonItemWriterBuilder}
	 * @see BigQueryLoadJobJsonItemWriter#setDatasetInfo(DatasetInfo)
	 */
	public BigQueryLoadJobJsonItemWriterBuilder<T> datasetInfo(final DatasetInfo datasetInfo) {
		this.datasetInfo = datasetInfo;
		return this;
	}

	/**
	 * Callback when {@link Job} will be finished.
	 * @param consumer your consumer
	 * @return {@link BigQueryLoadJobJsonItemWriterBuilder}
	 * @see BigQueryLoadJobJsonItemWriter#setJobConsumer(Consumer)
	 */
	public BigQueryLoadJobJsonItemWriterBuilder<T> jobConsumer(final Consumer<Job> consumer) {
		this.jobConsumer = consumer;
		return this;
	}

	/**
	 * Describes what should be written (format) and its destination (table).
	 * @param configuration BigQuery channel configuration
	 * @return {@link BigQueryLoadJobJsonItemWriterBuilder}
	 * @see BigQueryLoadJobJsonItemWriter#setWriteChannelConfig(WriteChannelConfiguration)
	 */
	public BigQueryLoadJobJsonItemWriterBuilder<T> writeChannelConfig(final WriteChannelConfiguration configuration) {
		this.writeChannelConfig = configuration;
		return this;
	}

	/**
	 * BigQuery service, responsible for API calls.
	 * @param bigQuery BigQuery service
	 * @return {@link BigQueryLoadJobJsonItemWriter}
	 * @see BigQueryLoadJobJsonItemWriter#setBigQuery(BigQuery)
	 */
	public BigQueryLoadJobJsonItemWriterBuilder<T> bigQuery(final BigQuery bigQuery) {
		this.bigQuery = bigQuery;
		return this;
	}

	/**
	 * Please remember about {@link BigQueryLoadJobJsonItemWriter#afterPropertiesSet()}.
	 * @return {@link BigQueryLoadJobJsonItemWriter}
	 */
	public BigQueryLoadJobJsonItemWriter<T> build() {
		final BigQueryLoadJobJsonItemWriter<T> writer = new BigQueryLoadJobJsonItemWriter<>();

		writer.setMarshaller(this.marshaller == null ? new JacksonJsonObjectMarshaller<>() : this.marshaller);
		writer.setBigQuery(this.bigQuery == null ? BigQueryOptions.getDefaultInstance().getService() : this.bigQuery);

		writer.setWriteChannelConfig(this.writeChannelConfig);
		writer.setJobConsumer(this.jobConsumer);
		writer.setDatasetInfo(this.datasetInfo);

		return writer;
	}

}