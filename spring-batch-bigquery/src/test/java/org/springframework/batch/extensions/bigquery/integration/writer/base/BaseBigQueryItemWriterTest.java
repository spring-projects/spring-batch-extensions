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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Method;
import java.util.Objects;

public abstract class BaseBigQueryItemWriterTest {

    protected static final String DATASET = "spring_extensions";

    protected final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    private static final String TABLE_PATTERN = "%s_%s";

    @BeforeEach
    void prepareTest(TestInfo testInfo) {
        if (Objects.isNull(bigQuery.getDataset(DATASET))) {
            bigQuery.create(DatasetInfo.of(DATASET));
        }

        if (Objects.isNull(bigQuery.getTable(DATASET, getTableName(testInfo)))) {
            TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
            bigQuery.create(TableInfo.of(TableId.of(DATASET, getTableName(testInfo)), tableDefinition));
        }
    }

    @AfterEach
    void cleanupTest(TestInfo testInfo) {
        bigQuery.delete(TableId.of(DATASET, getTableName(testInfo)));
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
                .newBuilder(TableId.of(DATASET, getTableName(testInfo)))
                .setSchema(PersonDto.getBigQuerySchema())
                .setAutodetect(false)
                .setFormatOptions(formatOptions)
                .build();
    }

    protected void waitForJobToFinish(JobId jobId) {
        JobStatus status = bigQuery.getJob(jobId).getStatus();

        while (BooleanUtils.isFalse(JobStatus.State.DONE.equals(status.getState()))) {
            status = bigQuery.getJob(jobId).getStatus();
        }
    }

    @JsonPropertyOrder(value = {"name", "age"})
    public record PersonDto(String name, Integer age) {

        public static Schema getBigQuerySchema() {
            Field nameField = Field.newBuilder("name", StandardSQLTypeName.STRING).build();
            Field ageField = Field.newBuilder("age", StandardSQLTypeName.INT64).build();
            return Schema.of(nameField, ageField);
        }

    }

}
