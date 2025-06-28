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

package org.springframework.batch.extensions.bigquery.reader.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A builder for {@link BigQueryQueryItemReader}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/reader/builder/BigQueryInteractiveQueryItemReaderBuilderTests.java">Examples</a>
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/reader/builder/BigQueryBatchQueryItemReaderBuilderTests.java">Examples</a>
 */
public class BigQueryQueryItemReaderBuilder<T> {

    private BigQuery bigQuery;
    private String query;
    private Converter<FieldValueList, T> rowMapper;
    private QueryJobConfiguration jobConfiguration;
    private Class<T> targetType;

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     * @return {@link BigQueryQueryItemReaderBuilder}
     * @see BigQueryQueryItemReader#setBigQuery(BigQuery)
     */
    public BigQueryQueryItemReaderBuilder<T> bigQuery(final BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        return this;
    }

    /**
     * Schema of the query: {@code SELECT <column> FROM <dataset>.<table>}.
     * <p>
     * It is really recommended to use {@code LIMIT n}
     * because BigQuery charges you for the amount of data that is being processed.
     *
     * @param query your query to run
     * @return {@link BigQueryQueryItemReaderBuilder}
     * @see BigQueryQueryItemReader#setJobConfiguration(QueryJobConfiguration)
     */
    public BigQueryQueryItemReaderBuilder<T> query(final String query) {
        this.query = query;
        return this;
    }

    /**
     * Row mapper which transforms single BigQuery row into a desired type.
     *
     * @param rowMapper your row mapper
     * @return {@link BigQueryQueryItemReaderBuilder}
     * @see BigQueryQueryItemReader#setRowMapper(Converter)
     */
    public BigQueryQueryItemReaderBuilder<T> rowMapper(final Converter<FieldValueList, T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    /**
     * Specifies query to run, destination table, etc.
     *
     * @param jobConfiguration BigQuery job configuration
     * @return {@link BigQueryQueryItemReaderBuilder}
     * @see BigQueryQueryItemReader#setJobConfiguration(QueryJobConfiguration)
     */
    public BigQueryQueryItemReaderBuilder<T> jobConfiguration(final QueryJobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
        return this;
    }

    /**
     * Specifies a target type which will be used as a result.
     * Only needed when {@link BigQueryQueryItemReaderBuilder#rowMapper} is not provided.
     * Take into account that only {@link Class#isRecord()} supported.
     *
     * @param targetType a {@link Class} that represent desired type
     * @return {@link BigQueryQueryItemReaderBuilder}
     */
    public BigQueryQueryItemReaderBuilder<T> targetType(final Class<T> targetType) {
        this.targetType = targetType;
        return this;
    }

    /**
     * Please remember about {@link BigQueryQueryItemReader#afterPropertiesSet()}.
     *
     * @return {@link BigQueryQueryItemReader}
     */
    public BigQueryQueryItemReader<T> build() {
        final BigQueryQueryItemReader<T> reader = new BigQueryQueryItemReader<>();

        reader.setBigQuery(this.bigQuery == null ? BigQueryOptions.getDefaultInstance().getService() : this.bigQuery);

        if (this.rowMapper == null) {
            Assert.notNull(this.targetType, "No target type provided");
            Assert.isTrue(this.targetType.isRecord(), "Only Java record supported");
            reader.setRowMapper(new RecordMapper<T>().generateMapper(this.targetType));
        } else {
            reader.setRowMapper(this.rowMapper);
        }

        if (this.jobConfiguration == null) {
            Assert.isTrue(StringUtils.hasText(this.query), "No query provided");
            reader.setJobConfiguration(QueryJobConfiguration.newBuilder(this.query).build());
        } else {
            reader.setJobConfiguration(this.jobConfiguration);
        }

        return reader;
    }

}