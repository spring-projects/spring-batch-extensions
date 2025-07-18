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

package org.springframework.batch.extensions.bigquery.writer.loadjob.parquet.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.avro.Schema;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.batch.extensions.bigquery.writer.loadjob.parquet.BigQueryLoadJobParquetItemWriter;

import java.util.function.Consumer;

/**
 * A builder for {@link BigQueryLoadJobParquetItemWriter}.
 *
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href=
 * "https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/loadjob/parquet/builder/BigQueryLoadJobParquetItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryLoadJobParquetItemWriterBuilder {

	private Schema schema;

	private CompressionCodecName codecName;

	private Consumer<Job> jobConsumer;

	private DatasetInfo datasetInfo;

	private WriteChannelConfiguration writeChannelConfig;

	private BigQuery bigQuery;

	/**
	 * Instructs which fields are expected.
	 * @param schema your schema
	 * @return {@link BigQueryLoadJobParquetItemWriterBuilder}
	 * @see BigQueryLoadJobParquetItemWriter#setSchema(Schema)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder schema(final Schema schema) {
		this.schema = schema;
		return this;
	}

	/**
	 * Instructs what is the expected compression algorithm.
	 * @param codecName your codec
	 * @return {@link BigQueryLoadJobParquetItemWriterBuilder}
	 * @see BigQueryLoadJobParquetItemWriter#setSchema(Schema)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder codecName(final CompressionCodecName codecName) {
		this.codecName = codecName;
		return this;
	}

	/**
	 * Provides additional information about the
	 * {@link com.google.cloud.bigquery.Dataset}.
	 * @param datasetInfo BigQuery dataset info
	 * @return {@link BigQueryLoadJobParquetItemWriterBuilder}
	 * @see BigQueryLoadJobParquetItemWriter#setDatasetInfo(DatasetInfo)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder datasetInfo(DatasetInfo datasetInfo) {
		this.datasetInfo = datasetInfo;
		return this;
	}

	/**
	 * Callback when {@link Job} will be finished.
	 * @param consumer your consumer
	 * @return {@link BigQueryLoadJobParquetItemWriterBuilder}
	 * @see BigQueryLoadJobParquetItemWriter#setJobConsumer(Consumer)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder jobConsumer(Consumer<Job> consumer) {
		this.jobConsumer = consumer;
		return this;
	}

	/**
	 * Describes what should be written (format) and its destination (table).
	 * @param configuration BigQuery channel configuration
	 * @return {@link BigQueryLoadJobParquetItemWriterBuilder}
	 * @see BigQueryLoadJobParquetItemWriter#setWriteChannelConfig(WriteChannelConfiguration)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder writeChannelConfig(WriteChannelConfiguration configuration) {
		this.writeChannelConfig = configuration;
		return this;
	}

	/**
	 * BigQuery service, responsible for API calls.
	 * @param bigQuery BigQuery service
	 * @return {@link BigQueryLoadJobParquetItemWriter}
	 * @see BigQueryLoadJobParquetItemWriter#setBigQuery(BigQuery)
	 */
	public BigQueryLoadJobParquetItemWriterBuilder bigQuery(BigQuery bigQuery) {
		this.bigQuery = bigQuery;
		return this;
	}

	/**
	 * Please remember about
	 * {@link BigQueryLoadJobParquetItemWriter#afterPropertiesSet()}.
	 * @return {@link BigQueryLoadJobParquetItemWriter}
	 */
	public BigQueryLoadJobParquetItemWriter build() {
		BigQueryLoadJobParquetItemWriter writer = new BigQueryLoadJobParquetItemWriter();

		writer.setCodecName(this.codecName == null ? CompressionCodecName.UNCOMPRESSED : this.codecName);
		writer.setBigQuery(this.bigQuery == null ? BigQueryOptions.getDefaultInstance().getService() : this.bigQuery);

		writer.setSchema(this.schema);
		writer.setWriteChannelConfig(this.writeChannelConfig);
		writer.setJobConsumer(this.jobConsumer);
		writer.setDatasetInfo(this.datasetInfo);

		return writer;
	}

}