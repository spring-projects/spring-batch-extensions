package org.springframework.batch.extensions.bigquery.emulator.reader;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.BigQueryQueryItemReader;
import org.springframework.batch.extensions.bigquery.reader.builder.BigQueryQueryItemReaderBuilder;

class BigQueryEmulatorItemReaderTest extends BaseEmulatorItemReaderTest {

    @Test
    void testBatchReader() throws Exception {
        QueryJobConfiguration jobConfiguration = QueryJobConfiguration
                .newBuilder("SELECT p.name, p.age FROM spring_batch_extensions.csv p ORDER BY p.name LIMIT 2")
                .setDestinationTable(TableId.of(TestConstants.DATASET, TestConstants.CSV))
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
                .setDestinationTable(TableId.of(TestConstants.DATASET, TestConstants.CSV))
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