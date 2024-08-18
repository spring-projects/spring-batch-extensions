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

package org.springframework.batch.extensions.bigquery.gcloud.reader;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.*;
import org.springframework.batch.extensions.bigquery.common.BigQueryDataLoader;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.gcloud.base.BaseBigQueryGcloudIntegrationTest;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;

class BigQueryGcloudItemReaderTest extends BaseBigQueryGcloudIntegrationTest {

    @BeforeAll
    static void init() throws Exception {
        if (BIG_QUERY.getDataset(TestConstants.DATASET) == null) {
            BIG_QUERY.create(DatasetInfo.of(TestConstants.DATASET));
        }

        if (BIG_QUERY.getTable(TestConstants.DATASET, TestConstants.CSV) == null) {
            TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
            BIG_QUERY.create(TableInfo.of(TableId.of(TestConstants.DATASET, TestConstants.CSV), tableDefinition));
        }

        new BigQueryDataLoader(BIG_QUERY).loadCsvSample(TestConstants.CSV);
    }

    @AfterAll
    static void cleanupTest() {
        BIG_QUERY.delete(TableId.of(TestConstants.DATASET, TestConstants.CSV));
    }

    @Test
    void testBatchQuery() throws Exception {
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.%s p ORDER BY p.name LIMIT 2".formatted(TestConstants.CSV))
                .setDestinationTable(TableId.of(TestConstants.DATASET, TestConstants.CSV))
                .setPriority(QueryJobConfiguration.Priority.BATCH)
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(BIG_QUERY)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .jobConfiguration(jobConfiguration)
                .build();

        reader.afterPropertiesSet();

        verifyResult(reader);
    }

    @Test
    void testInteractiveQuery() throws Exception {
        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(BIG_QUERY)
                .query("SELECT p.name, p.age FROM spring_batch_extensions.%s p ORDER BY p.name LIMIT 2".formatted(TestConstants.CSV))
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        reader.afterPropertiesSet();

        verifyResult(reader);
    }

    private void verifyResult(BigQueryQueryItemReader<PersonDto> reader) throws Exception {
        PersonDto actualFirstPerson = reader.read();
        PersonDto expectedFirstPerson = BigQueryDataLoader.CHUNK.getItems().get(0);

        PersonDto actualSecondPerson = reader.read();
        PersonDto expectedSecondPerson = BigQueryDataLoader.CHUNK.getItems().get(1);

        PersonDto actualThirdPerson = reader.read();

        Assertions.assertNotNull(actualFirstPerson);
        Assertions.assertEquals(expectedFirstPerson.name(), actualFirstPerson.name());
        Assertions.assertEquals(0, expectedFirstPerson.age().compareTo(actualFirstPerson.age()));

        Assertions.assertNotNull(actualSecondPerson);
        Assertions.assertEquals(expectedSecondPerson.name(), actualSecondPerson.name());
        Assertions.assertEquals(0, expectedSecondPerson.age().compareTo(actualSecondPerson.age()));

        Assertions.assertNull(actualThirdPerson);
    }

}