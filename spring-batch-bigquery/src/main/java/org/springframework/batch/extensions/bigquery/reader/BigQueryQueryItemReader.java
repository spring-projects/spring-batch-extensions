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

package org.springframework.batch.extensions.bigquery.reader;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Iterator;

/**
 * BigQuery {@link ItemReader} that accepts simple query as the input.
 * <p>
 * Internally BigQuery Java library creates a {@link com.google.cloud.bigquery.JobConfiguration.Type#QUERY} job.
 * Which means that result is coming asynchronously.
 * <p>
 * Also, worth mentioning that you should take into account concurrency limits.
 * <p>
 * Results of this query by default are stored in the shape of a temporary table.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://cloud.google.com/bigquery/docs/running-queries#queries">Interactive queries</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/running-queries#batch">Batch queries</a>
 * @see <a href="https://cloud.google.com/bigquery/quotas#concurrent_rate_interactive_queries">Concurrency limits</a>
 */
public class BigQueryQueryItemReader<T> implements ItemReader<T>, InitializingBean {

    private final Log logger = LogFactory.getLog(getClass());

    private BigQuery bigQuery;
    private Converter<FieldValueList, T> rowMapper;
    private QueryJobConfiguration jobConfiguration;
    private Iterator<FieldValueList> iterator;

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     */
    public void setBigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    /**
     * Row mapper which transforms single BigQuery row into desired type.
     *
     * @param rowMapper your row mapper
     */
    public void setRowMapper(Converter<FieldValueList, T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    /**
     * Specifies query to run, destination table, etc.
     *
     * @param jobConfiguration BigQuery job configuration
     */
    public void setJobConfiguration(QueryJobConfiguration jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
    }

    @Override
    public T read() throws Exception {
        if (iterator == null) {
            doOpen();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Reading next element");
        }

        return iterator.hasNext() ? rowMapper.convert(iterator.next()) : null;
    }

    private void doOpen() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing query");
        }
        iterator = bigQuery.query(jobConfiguration).getValues().iterator();
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(this.bigQuery, "BigQuery service must be provided");
        Assert.notNull(this.rowMapper, "Row mapper must be provided");
        Assert.notNull(this.jobConfiguration, "Job configuration must be provided");
    }

}