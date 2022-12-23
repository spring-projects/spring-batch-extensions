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

package org.springframework.batch.extensions.bigquery.common;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.springframework.batch.extensions.bigquery.writer.BigQueryCsvItemWriter;
import org.springframework.batch.extensions.bigquery.writer.BigQueryJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryCsvItemWriterBuilder;
import org.springframework.batch.extensions.bigquery.writer.builder.BigQueryJsonItemWriterBuilder;
import org.springframework.batch.item.Chunk;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class BigQueryDataLoader {

    /** Order must be defined so later executed queries results could be predictable */
    private static final List<PersonDto> PERSONS = Stream
            .of(new PersonDto("Volodymyr", 27), new PersonDto("Oleksandra", 26))
            .sorted(Comparator.comparing(PersonDto::name))
            .toList();

    public static final Chunk<PersonDto> CHUNK = new Chunk<>(PERSONS);

    private final BigQuery bigQuery;

    public BigQueryDataLoader(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    public void loadCsvSample(String tableName) throws Exception {
        AtomicReference<Job> job = new AtomicReference<>();

        WriteChannelConfiguration channelConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, tableName))
                .setSchema(PersonDto.getBigQuerySchema())
                .setAutodetect(false)
                .setFormatOptions(FormatOptions.csv())
                .build();

        BigQueryCsvItemWriter<PersonDto> writer = new BigQueryCsvItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(channelConfiguration)
                .jobConsumer(job::set)
                .build();

        writer.afterPropertiesSet();
        writer.write(CHUNK);
        job.get().waitFor();
    }

    public void loadJsonSample(String tableName) throws Exception {
        AtomicReference<Job> job = new AtomicReference<>();

        WriteChannelConfiguration channelConfiguration = WriteChannelConfiguration
                .newBuilder(TableId.of(TestConstants.DATASET, tableName))
                .setSchema(PersonDto.getBigQuerySchema())
                .setAutodetect(false)
                .setFormatOptions(FormatOptions.json())
                .build();

        BigQueryJsonItemWriter<PersonDto> writer = new BigQueryJsonItemWriterBuilder<PersonDto>()
                .bigQuery(bigQuery)
                .writeChannelConfig(channelConfiguration)
                .jobConsumer(job::set)
                .build();

        writer.afterPropertiesSet();
        writer.write(CHUNK);
        job.get().waitFor();
    }

}
