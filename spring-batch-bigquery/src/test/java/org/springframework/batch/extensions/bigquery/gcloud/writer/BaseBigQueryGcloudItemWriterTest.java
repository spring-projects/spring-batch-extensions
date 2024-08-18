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

package org.springframework.batch.extensions.bigquery.gcloud.writer;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Assertions;
import org.springframework.batch.extensions.bigquery.common.BigQueryDataLoader;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.gcloud.base.BaseBigQueryGcloudIntegrationTest;

abstract class BaseBigQueryGcloudItemWriterTest extends BaseBigQueryGcloudIntegrationTest {

    protected void verifyResults(String tableName) {
        Dataset dataset = BIG_QUERY.getDataset(TestConstants.DATASET);
        Table table = BIG_QUERY.getTable(TableId.of(TestConstants.DATASET, tableName));
        TableId tableId = table.getTableId();
        TableResult tableResult = BIG_QUERY.listTableData(tableId, BigQuery.TableDataListOption.pageSize(2L));

        Assertions.assertNotNull(dataset.getDatasetId());
        Assertions.assertNotNull(tableId);
        Assertions.assertEquals(BigQueryDataLoader.CHUNK.size(), tableResult.getTotalRows());

        tableResult
                .getValues()
                .forEach(field -> {
                    Assertions.assertTrue(
                            BigQueryDataLoader.CHUNK.getItems().stream().map(PersonDto::name).anyMatch(name -> field.get(0).getStringValue().equals(name))
                    );

                    boolean ageCondition = BigQueryDataLoader.CHUNK
                            .getItems()
                            .stream()
                            .map(PersonDto::age)
                            .map(Long::valueOf)
                            .anyMatch(age -> age.compareTo(field.get(1).getLongValue()) == 0);

                    Assertions.assertTrue(ageCondition);
                });
    }

}