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

package org.springframework.batch.extensions.bigquery.writer.loadjob.csv.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.springframework.batch.extensions.bigquery.writer.loadjob.csv.BigQueryLoadJobCsvItemWriter;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Consumer;

/**
 * A builder for {@link BigQueryLoadJobCsvItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href=
 * "https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/builder/BigQueryCsvItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryCsvItemWriterBuilder<T> {

	private Converter<T, byte[]> rowMapper;

	private Consumer<Job> jobConsumer;

	private DatasetInfo datasetInfo;

	private WriteChannelConfiguration writeChannelConfig;

	private BigQuery bigQuery;

	/**
	 * Default constructor
	 */
	public BigQueryCsvItemWriterBuilder() {
	}

	/**
	 * Row mapper which transforms single BigQuery row into desired type.
	 * @param rowMapper your row mapper
	 * @return {@link BigQueryCsvItemWriterBuilder}
	 * @see BigQueryLoadJobCsvItemWriter#setRowMapper(Converter)
	 */
	public BigQueryCsvItemWriterBuilder<T> rowMapper(final Converter<T, byte[]> rowMapper) {
		this.rowMapper = rowMapper;
		return this;
	}

	/**
	 * Provides additional information about the
	 * {@link com.google.cloud.bigquery.Dataset}.
	 * @param datasetInfo BigQuery dataset info
	 * @return {@link BigQueryCsvItemWriterBuilder}
	 * @see BigQueryLoadJobCsvItemWriter#setDatasetInfo(DatasetInfo)
	 */
	public BigQueryCsvItemWriterBuilder<T> datasetInfo(final DatasetInfo datasetInfo) {
		this.datasetInfo = datasetInfo;
		return this;
	}

	/**
	 * Callback when {@link Job} will be finished.
	 * @param consumer your consumer
	 * @return {@link BigQueryCsvItemWriterBuilder}
	 * @see BigQueryLoadJobCsvItemWriter#setJobConsumer(Consumer)
	 */
	public BigQueryCsvItemWriterBuilder<T> jobConsumer(final Consumer<Job> consumer) {
		this.jobConsumer = consumer;
		return this;
	}

	/**
	 * Describes what should be written (format) and its destination (table).
	 * @param configuration BigQuery channel configuration
	 * @return {@link BigQueryCsvItemWriterBuilder}
	 * @see BigQueryLoadJobCsvItemWriter#setWriteChannelConfig(WriteChannelConfiguration)
	 */
	public BigQueryCsvItemWriterBuilder<T> writeChannelConfig(final WriteChannelConfiguration configuration) {
		this.writeChannelConfig = configuration;
		return this;
	}

	/**
	 * BigQuery service, responsible for API calls.
	 * @param bigQuery BigQuery service
	 * @return {@link BigQueryCsvItemWriterBuilder}
	 * @see BigQueryLoadJobCsvItemWriter#setBigQuery(BigQuery)
	 */
	public BigQueryCsvItemWriterBuilder<T> bigQuery(final BigQuery bigQuery) {
		this.bigQuery = bigQuery;
		return this;
	}

	/**
	 * Please remember about {@link BigQueryLoadJobCsvItemWriter#afterPropertiesSet()}.
	 * @return {@link BigQueryLoadJobCsvItemWriter}
	 */
	public BigQueryLoadJobCsvItemWriter<T> build() {
		final BigQueryLoadJobCsvItemWriter<T> writer = new BigQueryLoadJobCsvItemWriter<>();

		writer.setBigQuery(this.bigQuery == null ? BigQueryOptions.getDefaultInstance().getService() : this.bigQuery);

		writer.setRowMapper(this.rowMapper);
		writer.setWriteChannelConfig(this.writeChannelConfig);
		writer.setJobConsumer(this.jobConsumer);
		writer.setDatasetInfo(this.datasetInfo);

		return writer;
	}

}