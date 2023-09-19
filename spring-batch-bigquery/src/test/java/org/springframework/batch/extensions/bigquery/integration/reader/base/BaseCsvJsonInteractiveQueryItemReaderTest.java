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

package org.springframework.batch.extensions.bigquery.integration.reader.base;

import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.integration.base.BaseBigQueryIntegrationTest;

import java.util.Objects;

public abstract class BaseCsvJsonInteractiveQueryItemReaderTest extends BaseBigQueryIntegrationTest {

    @BeforeEach
    void prepareTest(TestInfo testInfo) {
        if (Objects.isNull(bigQuery.getDataset(TestConstants.DATASET))) {
            bigQuery.create(DatasetInfo.of(TestConstants.DATASET));
        }

        String tableName = getTableName(testInfo);

        if (Objects.isNull(bigQuery.getTable(TestConstants.DATASET, tableName))) {
            TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
            bigQuery.create(TableInfo.of(TableId.of(TestConstants.DATASET, tableName), tableDefinition));
        }
    }

    @AfterEach
    void cleanupTest(TestInfo testInfo) {
        bigQuery.delete(TableId.of(TestConstants.DATASET, getTableName(testInfo)));
    }

}
