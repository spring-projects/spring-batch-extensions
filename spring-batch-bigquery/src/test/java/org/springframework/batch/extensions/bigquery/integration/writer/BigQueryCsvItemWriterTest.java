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

package org.springframework.batch.extensions.bigquery.integration.writer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.apache.commons.lang3.math.NumberUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.extensions.bigquery.integration.writer.base.BaseBigQueryItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;
import org.springframework.batch.item.Chunk;

import java.util.concurrent.atomic.AtomicReference;

@Tag("csv")
public class BigQueryCsvItemWriterTest extends BaseBigQueryItemWriterTest {

    @Test
    void test1(TestInfo testInfo) throws Exception {
        AtomicReference<JobId> jobId = new AtomicReference<>();

        BigQueryCsvItemWriter writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(generateConfiguration(testInfo, FormatOptions.csv()))
                .jobConsumer(j -> jobId.set(j.getJobId()))
                .build();

        writer.afterPropertiesSet();

        Chunk<PersonDto> chunk = Chunk.of(new PersonDto("Volodymyr", 27), new PersonDto("Oleksandra", 26));
        writer.write(chunk);

        waitForJobToFinish(jobId.get());

        Dataset dataset = bigQuery.getDataset(DATASET);
        Table table = bigQuery.getTable(TableId.of(DATASET, getTableName(testInfo)));
        TableId tableId = table.getTableId();
        TableResult tableResult = bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(2L));

        Assertions.assertNotNull(dataset.getDatasetId());
        Assertions.assertNotNull(tableId);
        Assertions.assertEquals(chunk.size(), tableResult.getTotalRows());

        tableResult
                .getValues()
                .forEach(field -> {
                    Assertions.assertTrue(
                            chunk.getItems().stream().map(PersonDto::name).anyMatch(name -> field.get(NumberUtils.INTEGER_ZERO).getStringValue().equals(name))
                    );

                    boolean ageCondition = chunk
                            .getItems()
                            .stream()
                            .map(PersonDto::age)
                            .map(Long::valueOf)
                            .anyMatch(age -> age.compareTo(field.get(NumberUtils.INTEGER_ONE).getLongValue()) == NumberUtils.INTEGER_ZERO);

                    Assertions.assertTrue(ageCondition);
                });
    }

}
