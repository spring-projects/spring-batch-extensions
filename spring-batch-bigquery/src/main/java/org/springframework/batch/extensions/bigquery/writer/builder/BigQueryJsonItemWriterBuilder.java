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

package org.springframework.batch.extensions.bigquery.writer.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.item.json.JsonObjectMarshaller;

import java.util.function.Consumer;

/**
 * A builder for {@link BigQueryJsonItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/builder/BigQueryJsonItemWriterBuilderTests.java">Examples</a>
 */
public class BigQueryJsonItemWriterBuilder<T>  {

    private JsonObjectMarshaller<T> marshaller;
    private Consumer<Job> jobConsumer;
    private DatasetInfo datasetInfo;
    private WriteChannelConfiguration writeChannelConfig;
    private BigQuery bigQuery;

    /**
     * Converts your DTO into a {@link String}.
     *
     * @param marshaller your mapper
     * @return {@link BigQueryJsonItemWriter}
     * @see BigQueryJsonItemWriter#setMarshaller(JsonObjectMarshaller)
     */
    public BigQueryJsonItemWriterBuilder<T> marshaller(JsonObjectMarshaller<T> marshaller) {
        this.marshaller = marshaller;
        return this;
    }

    /**
     * Provides additional information about the {@link com.google.cloud.bigquery.Dataset}.
     *
     * @param datasetInfo BigQuery dataset info
     * @return {@link BigQueryJsonItemWriter}
     * @see BigQueryJsonItemWriter#setDatasetInfo(DatasetInfo)
     */
    public BigQueryJsonItemWriterBuilder<T> datasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
        return this;
    }

    /**
     * Callback when {@link Job} will be finished.
     *
     * @param consumer your consumer
     * @return {@link BigQueryJsonItemWriter}
     * @see BigQueryJsonItemWriter#setJobConsumer(Consumer)
     */
    public BigQueryJsonItemWriterBuilder<T> jobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
        return this;
    }

    /**
     * Describes what should be written (format) and its destination (table).
     *
     * @param configuration BigQuery channel configuration
     * @return {@link BigQueryJsonItemWriter}
     * @see BigQueryJsonItemWriter#setWriteChannelConfig(WriteChannelConfiguration)
     */
    public BigQueryJsonItemWriterBuilder<T> writeChannelConfig(WriteChannelConfiguration configuration) {
        this.writeChannelConfig = configuration;
        return this;
    }

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     * @return {@link BigQueryJsonItemWriter}
     * @see BigQueryJsonItemWriter#setBigQuery(BigQuery)
     */
    public BigQueryJsonItemWriterBuilder<T> bigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        return this;
    }

    /**
     * Please remember about {@link BigQueryJsonItemWriter#afterPropertiesSet()}.
     *
     * @return {@link BigQueryJsonItemWriter}
     */
    public BigQueryJsonItemWriter<T> build() {
        BigQueryJsonItemWriter<T> writer = new BigQueryJsonItemWriter<>();

        writer.setMarshaller(this.marshaller);
        writer.setWriteChannelConfig(this.writeChannelConfig);
        writer.setJobConsumer(this.jobConsumer);
        writer.setBigQuery(this.bigQuery);
        writer.setDatasetInfo(this.datasetInfo);

        return writer;
    }

}