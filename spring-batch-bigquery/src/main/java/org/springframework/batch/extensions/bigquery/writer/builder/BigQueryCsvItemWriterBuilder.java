/*
 * Copyright 2002-2022 the original author or authors.
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
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see BigQueryCsvItemWriter
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/builder/BigQueryCsvItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryCsvItemWriterBuilder<T>  {

    private Converter<T, byte[]> rowMapper;

    private Consumer<Job> jobConsumer;
    private DatasetInfo datasetInfo;
    private WriteChannelConfiguration writeChannelConfig;
    private BigQuery bigQuery;

    public BigQueryCsvItemWriterBuilder<T> rowMapper(Converter<T, byte[]> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public BigQueryCsvItemWriterBuilder<T> datasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
        return this;
    }

    public BigQueryCsvItemWriterBuilder<T> jobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
        return this;
    }

    public BigQueryCsvItemWriterBuilder<T> writeChannelConfig(WriteChannelConfiguration configuration) {
        this.writeChannelConfig = configuration;
        return this;
    }

    public BigQueryCsvItemWriterBuilder<T> bigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        return this;
    }

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
