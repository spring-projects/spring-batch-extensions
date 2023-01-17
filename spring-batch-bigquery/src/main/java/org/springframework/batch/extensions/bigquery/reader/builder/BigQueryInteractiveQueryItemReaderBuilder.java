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

package org.springframework.batch.extensions.bigquery.reader.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.extensions.bigquery.reader.BigQueryInteractiveQueryItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * A builder for {@link BigQueryInteractiveQueryItemReader}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/reader/builder/BigQueryInteractiveQueryItemReaderBuilderTests.java">Examples</a>
 */
public class BigQueryInteractiveQueryItemReaderBuilder<T> {

    private BigQuery bigQuery;
    private String query;
    private Converter<FieldValueList, T> rowMapper;
    private QueryJobConfiguration jobConfiguration;

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     * @return {@link BigQueryInteractiveQueryItemReaderBuilder}
     * @see BigQueryInteractiveQueryItemReader#setBigQuery(BigQuery)
     */
    public BigQueryInteractiveQueryItemReaderBuilder<T> bigQuery(BigQuery bigQuery) {
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
     * @return {@link BigQueryInteractiveQueryItemReaderBuilder}
     * @see BigQueryInteractiveQueryItemReader#setJobConfiguration(QueryJobConfiguration)
     */
    public BigQueryInteractiveQueryItemReaderBuilder<T> query(String query) {
        this.query = query;
        return this;
    }

    /**
     * Row mapper which transforms single BigQuery row into desired type.
     *
     * @param rowMapper your row mapper
     * @return {@link BigQueryInteractiveQueryItemReaderBuilder}
     * @see BigQueryInteractiveQueryItemReader#setRowMapper(Converter)
     */
    public BigQueryInteractiveQueryItemReaderBuilder<T> rowMapper(Converter<FieldValueList, T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    /**
     * Specifies query to run, destination table, etc.
     *
     * @param jobConfiguration BigQuery job configuration
     * @return {@link BigQueryInteractiveQueryItemReaderBuilder}
     * @see BigQueryInteractiveQueryItemReader#setJobConfiguration(QueryJobConfiguration)
     */
    public BigQueryInteractiveQueryItemReaderBuilder<T> jobConfiguration(QueryJobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
        return this;
    }

    /**
     * Please do not forget about {@link BigQueryInteractiveQueryItemReader#afterPropertiesSet()}.
     *
     * @return {@link BigQueryInteractiveQueryItemReader}
     */
    public BigQueryInteractiveQueryItemReader<T> build() {
        BigQueryInteractiveQueryItemReader<T> reader = new BigQueryInteractiveQueryItemReader<>();

        reader.setBigQuery(this.bigQuery);
        reader.setRowMapper(this.rowMapper);

        if (Objects.nonNull(this.jobConfiguration)) {
            reader.setJobConfiguration(this.jobConfiguration);
        } else {
            Assert.isTrue(StringUtils.isNotBlank(this.query), "No query provided");
            reader.setJobConfiguration(QueryJobConfiguration.newBuilder(this.query).build());
        }

        return reader;
    }

}
