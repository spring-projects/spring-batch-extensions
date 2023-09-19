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

package org.springframework.batch.extensions.bigquery.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.cloud.bigquery.Table;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * CSV writer for BigQuery.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://en.wikipedia.org/wiki/Comma-separated_values">CSV</a>
 */
public class BigQueryCsvItemWriter<T> extends BigQueryBaseItemWriter<T> implements InitializingBean {

    private Converter<T, byte[]> rowMapper;
    private ObjectWriter objectWriter;
    private Class itemClass;

    /**
     * Actual type of incoming data can be obtained only in runtime
     */
    @Override
    protected synchronized void doInitializeProperties(List<? extends T> items) {
        if (Objects.isNull(this.itemClass)) {
            T firstItem = items.stream().findFirst().orElseThrow(RuntimeException::new);
            this.itemClass = firstItem.getClass();

            if (Objects.isNull(this.rowMapper)) {
                this.objectWriter = new CsvMapper().writerWithTypedSchemaFor(this.itemClass);
            }

            logger.debug("Writer setup is completed");
        }
    }

    /**
     * Row mapper which transforms single BigQuery row into desired type.
     *
     * @param rowMapper your row mapper
     */
    public void setRowMapper(Converter<T, byte[]> rowMapper) {
        this.rowMapper = rowMapper;
    }


    @Override
    protected List<byte[]> convertObjectsToByteArrays(List<? extends T> items) {
        return items
                .stream()
                .map(this::mapItemToCsv)
                .filter(ArrayUtils::isNotEmpty)
                .map(String::new)
                .filter(Predicate.not(ObjectUtils::isEmpty))
                .map(row -> row.getBytes(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    @Override
    public void afterPropertiesSet() {
        super.baseAfterPropertiesSet(() -> {
            Table table = getTable();

            if (BooleanUtils.toBoolean(super.writeChannelConfig.getAutodetect())) {
                if ((tableHasDefinedSchema(table) && super.logger.isWarnEnabled())) {
                    super.logger.warn("Mixing autodetect mode with already defined schema may lead to errors on BigQuery side");
                }
            } else {
                Assert.notNull(super.writeChannelConfig.getSchema(), "Schema must be provided");

                if (tableHasDefinedSchema(table)) {
                    Assert.isTrue(
                            table.getDefinition().getSchema().equals(super.writeChannelConfig.getSchema()),
                            "Schema should be the same"
                    );
                }
            }

            return null;
        });
    }

    private byte[] mapItemToCsv(T t) {
        try {
            return Objects.isNull(rowMapper) ? objectWriter.writeValueAsBytes(t) : rowMapper.convert(t);
        }
        catch (JsonProcessingException e) {
            logger.error("Error during processing of the line: ", e);
            return null;
        }
    }

}
