/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.writer.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Consumer;

/**
 * A builder for {@link BigQueryCsvItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/builder/BigQueryCsvItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryCsvItemWriterBuilder<T>  {

    private Converter<T, byte[]> rowMapper;

    private Consumer<Job> jobConsumer;
    private DatasetInfo datasetInfo;
    private WriteChannelConfiguration writeChannelConfig;
    private BigQuery bigQuery;

    /**
     * Row mapper which transforms single BigQuery row into desired type.
     *
     * @param rowMapper your row mapper
     * @return {@link BigQueryCsvItemWriterBuilder}
     * @see BigQueryCsvItemWriter#setRowMapper(Converter)
     */
    public BigQueryCsvItemWriterBuilder<T> rowMapper(Converter<T, byte[]> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    /**
     * Provides additional information about the {@link com.google.cloud.bigquery.Dataset}.
     *
     * @param datasetInfo BigQuery dataset info
     * @return {@link BigQueryCsvItemWriterBuilder}
     * @see BigQueryCsvItemWriter#setDatasetInfo(DatasetInfo)
     */
    public BigQueryCsvItemWriterBuilder<T> datasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
        return this;
    }

    /**
     * Callback when {@link Job} will be finished.
     *
     * @param consumer your consumer
     * @return {@link BigQueryCsvItemWriterBuilder}
     * @see BigQueryCsvItemWriter#setJobConsumer(Consumer)
     */
    public BigQueryCsvItemWriterBuilder<T> jobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
        return this;
    }

    /**
     * Describes what should be written (format) and its destination (table).
     *
     * @param configuration BigQuery channel configuration
     * @return {@link BigQueryCsvItemWriterBuilder}
     * @see BigQueryCsvItemWriter#setWriteChannelConfig(WriteChannelConfiguration)
     */
    public BigQueryCsvItemWriterBuilder<T> writeChannelConfig(WriteChannelConfiguration configuration) {
        this.writeChannelConfig = configuration;
        return this;
    }

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     * @return {@link BigQueryCsvItemWriterBuilder}
     * @see BigQueryCsvItemWriter#setBigQuery(BigQuery)
     */
    public BigQueryCsvItemWriterBuilder<T> bigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        return this;
    }

    /**
     * Please do not forget about {@link BigQueryCsvItemWriter#afterPropertiesSet()}.
     *
     * @return {@link BigQueryCsvItemWriter}
     */
    public BigQueryCsvItemWriter<T> build() {
        BigQueryCsvItemWriter<T> writer = new BigQueryCsvItemWriter<>();

        writer.setRowMapper(this.rowMapper);
        writer.setWriteChannelConfig(this.writeChannelConfig);
        writer.setJobConsumer(this.jobConsumer);
        writer.setBigQuery(this.bigQuery);
        writer.setDatasetInfo(this.datasetInfo);

        return writer;
    }

}
