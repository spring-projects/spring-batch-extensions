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

package org.springframework.batch.extensions.bigquery.integration.writer.base;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;

import java.lang.reflect.Method;
import java.util.Objects;

public abstract class BaseBigQueryItemWriterTest {

    private static final String TABLE_PATTERN = "%s_%s";

    protected final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    @BeforeEach
    void prepareTest(TestInfo testInfo) {
        if (Objects.isNull(bigQuery.getDataset(TestConstants.DATASET))) {
            bigQuery.create(DatasetInfo.of(TestConstants.DATASET));
        }

        if (Objects.isNull(bigQuery.getTable(TestConstants.DATASET, getTableName(testInfo)))) {
            TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
            bigQuery.create(TableInfo.of(TableId.of(TestConstants.DATASET, getTableName(testInfo)), tableDefinition));
        }
    }

    @AfterEach
    void cleanupTest(TestInfo testInfo) {
        bigQuery.delete(TableId.of(TestConstants.DATASET, getTableName(testInfo)));
    }

    protected String getTableName(TestInfo testInfo) {
        return String.format(
                TABLE_PATTERN,
                testInfo.getTags().stream().findFirst().orElseThrow(),
                testInfo.getTestMethod().map(Method::getName).orElseThrow()
        );
    }

    protected WriteChannelConfiguration generateConfiguration(TestInfo testInfo, FormatOptions formatOptions) {
        return WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, getTableName(testInfo)))
                .setSchema(PersonDto.getBigQuerySchema())
                .setAutodetect(false)
                .setFormatOptions(formatOptions)
                .build();
    }

    /** TODO check {@link com.google.cloud.bigquery.Job#waitFor(RetryOption...)} */
    protected void waitForJobToFinish(JobId jobId) {
        JobStatus status = bigQuery.getJob(jobId).getStatus();

        while (BooleanUtils.isFalse(JobStatus.State.DONE.equals(status.getState()))) {
            status = bigQuery.getJob(jobId).getStatus();
        }
    }

}
