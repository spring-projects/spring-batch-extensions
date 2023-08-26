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

package org.springframework.batch.extensions.bigquery.integration.reader;

import org.apache.commons.lang3.math.NumberUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.extensions.bigquery.common.BigQueryDataLoader;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.integration.reader.base.BaseCsvJsonInteractiveQueryItemReaderTest;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;
import org.springframework.batch.item.Chunk;

@Tag("json")
public class BigQueryInteractiveQueryJsonItemReaderTest extends BaseCsvJsonInteractiveQueryItemReaderTest {

    @Test
    void interactiveQueryTest1(TestInfo testInfo) throws Exception {
        String tableName = getTableName(testInfo);
        new BigQueryDataLoader(bigQuery).loadJsonSample(tableName);
        Chunk<PersonDto> chunk = BigQueryDataLoader.CHUNK;

        BigQueryQueryItemReader<PersonDto> reader = new BigQueryQueryItemReaderBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .query(String.format("SELECT p.name, p.age FROM spring_batch_extensions.%s p ORDER BY p.name LIMIT 2", tableName))
                .rowMapper(TestConstants.PERSON_MAPPER)
                .build();

        reader.afterPropertiesSet();

        PersonDto actualFirstPerson = reader.read();
        PersonDto expectedFirstPerson = chunk.getItems().get(0);

        PersonDto actualSecondPerson = reader.read();
        PersonDto expectedSecondPerson = chunk.getItems().get(1);

        PersonDto actualThirdPerson = reader.read();

        Assertions.assertNotNull(actualFirstPerson);
        Assertions.assertEquals(expectedFirstPerson.name(), actualFirstPerson.name());
        Assertions.assertEquals(expectedFirstPerson.age().compareTo(actualFirstPerson.age()), NumberUtils.INTEGER_ZERO);

        Assertions.assertNotNull(actualSecondPerson);
        Assertions.assertEquals(expectedSecondPerson.name(), actualSecondPerson.name());
        Assertions.assertEquals(expectedSecondPerson.age().compareTo(actualSecondPerson.age()), NumberUtils.INTEGER_ZERO);

        Assertions.assertNull(actualThirdPerson);
    }

}
