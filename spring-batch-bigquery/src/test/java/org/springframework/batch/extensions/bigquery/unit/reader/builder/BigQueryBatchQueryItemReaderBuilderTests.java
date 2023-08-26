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

package org.springframework.batch.extensions.bigquery.unit.reader.builder;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;
import org.springframework.batch.extensions.bigquery.unit.base.AbstractBigQueryTest;

class BigQueryBatchQueryItemReaderBuilderTests extends AbstractBigQueryTest {

    @Test
    void testCustomQueryItemReader() {
        BigQuery mockedBigQuery = prepareMockedBigQuery();

        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.persons p LIMIT 2")
                .setDestinationTable(TableId.of(TestConstants.DATASET, "persons_duplicate"))
                .setPriority(QueryJobConfiguration.Priority.BATCH)
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(mockedBigQuery)
                .jobConfiguration(jobConfiguration)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        reader.afterPropertiesSet();

        Assertions.assertNotNull(reader);
    }

}
