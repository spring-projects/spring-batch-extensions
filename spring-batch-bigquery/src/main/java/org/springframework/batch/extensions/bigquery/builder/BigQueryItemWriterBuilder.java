/*
 * Copyright 2002-2021 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.builder;

import java.util.function.Consumer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;

import org.springframework.batch.extensions.bigquery.BigQueryItemWriter;
import org.springframework.core.convert.converter.Converter;

/**
 * A builder for {@link BigQueryItemWriter}.
 *
 * @author Vova Perebykivskyi
 * @since 0.1.0
 * @see BigQueryItemWriter
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/builder/BigQueryItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryItemWriterBuilder<T> {

    private Converter<T, byte[]> rowMapper;
    private Consumer<Job> jobConsumer;

    private DatasetInfo datasetInfo;
    private WriteChannelConfiguration writeChannelConfig;
    private BigQuery bigQuery;

    public BigQueryItemWriterBuilder<T> rowMapper(Converter<T, byte[]> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public BigQueryItemWriterBuilder<T> datasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
        return this;
    }

    public BigQueryItemWriterBuilder<T> jobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
        return this;
    }

    public BigQueryItemWriterBuilder<T> writeChannelConfig(WriteChannelConfiguration configuration) {
        this.writeChannelConfig = configuration;
        return this;
    }

    public BigQueryItemWriterBuilder<T> bigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        return this;
    }

    public BigQueryItemWriter<T> build() {
        BigQueryItemWriter<T> writer = new BigQueryItemWriter<>();

        writer.setRowMapper(this.rowMapper);
        writer.setWriteChannelConfig(this.writeChannelConfig);
        writer.setJobConsumer(this.jobConsumer);
        writer.setBigQuery(this.bigQuery);
        writer.setDatasetInfo(this.datasetInfo);

        return writer;
    }

}
