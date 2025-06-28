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

package org.springframework.batch.extensions.bigquery.emulator.reader;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.reader.base.EmulatorBaseItemReaderTest;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;

class EmulatorBigQueryItemReaderTest extends EmulatorBaseItemReaderTest {

    private static final TableId TABLE_ID = TableId.of(TestConstants.DATASET, TestConstants.CSV);

    @Test
    void testBatchReader() throws Exception {
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.csv p ORDER BY p.name LIMIT 2")
                .setDestinationTable(TABLE_ID)
                .setPriority(QueryJobConfiguration.Priority.BATCH)
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .jobConfiguration(jobConfiguration)
                .build();

        reader.afterPropertiesSet();

        verifyResult(reader);
    }

    @Test
    void testInteractiveReader() throws Exception {
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.csv p ORDER BY p.name LIMIT 2")
                .setDestinationTable(TABLE_ID)
                .build();

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .rowMapper(TestConstants.PERSON_MAPPER)
                .jobConfiguration(jobConfiguration)
                .build();

        reader.afterPropertiesSet();

        verifyResult(reader);
    }

    private void verifyResult(BigQueryQueryItemReader<PersonDto> reader) throws Exception {
        PersonDto actual1 = reader.read();
        Assertions.assertEquals("Volodymyr", actual1.name());
        Assertions.assertEquals(27, actual1.age());

        PersonDto actual2 = reader.read();
        Assertions.assertEquals("Oleksandra", actual2.name());
        Assertions.assertEquals(26, actual2.age());
    }
}