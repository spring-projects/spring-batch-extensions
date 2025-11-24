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

package org.springframework.batch.extensions.bigquery.writer.loadjob.csv;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Table;
import org.springframework.batch.extensions.bigquery.writer.loadjob.BigQueryLoadJobBaseItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.csv.CsvMapper;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * CSV writer for BigQuery.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://en.wikipedia.org/wiki/Comma-separated_values">CSV</a>
 */
public class BigQueryLoadJobCsvItemWriter<T> extends BigQueryLoadJobBaseItemWriter<T> {

	private Converter<T, byte[]> rowMapper;

	private ObjectWriter objectWriter;

	private Class<?> itemClass;

	/**
	 * Default constructor
	 */
	public BigQueryLoadJobCsvItemWriter() {
	}

	/**
	 * Actual type of incoming data can be obtained only in runtime
	 */
	@Override
	protected synchronized void doInitializeProperties(final List<? extends T> items) {
		if (this.itemClass == null) {
			final T firstItem = items.stream().findFirst().orElseThrow(() -> {
				logger.warn("Class type was not found");
				return new IllegalStateException("Class type was not found");
			});
			this.itemClass = firstItem.getClass();

			if (this.rowMapper == null) {
				this.objectWriter = new CsvMapper().writerWithTypedSchemaFor(this.itemClass);
			}

			logger.debug("Writer setup is completed");
		}
	}

	@Override
	protected List<byte[]> convertObjectsToByteArrays(final List<? extends T> items) {
		return items.stream().map(this::mapItemToCsv).filter(Predicate.not(ObjectUtils::isEmpty)).toList();
	}

	@Override
	protected void performFormatSpecificChecks() {
		final Table table = getTable();

		if (Boolean.TRUE.equals(super.writeChannelConfig.getAutodetect())) {
			if (tableHasDefinedSchema(table) && super.logger.isWarnEnabled()) {
				logger.warn("Mixing autodetect mode with already defined schema may lead to errors on BigQuery side");
			}
		}
		else {
			Assert.notNull(super.writeChannelConfig.getSchema(), "Schema must be provided");

			if (tableHasDefinedSchema(table)) {
				final boolean schemaEquals = Objects.equals(table.getDefinition().getSchema(),
						super.writeChannelConfig.getSchema());
				Assert.isTrue(schemaEquals, "Schema must be the same");
			}
		}

		final String format = FormatOptions.csv().getType();
		final boolean formatEquals = Objects.equals(format, super.writeChannelConfig.getFormat());
		Assert.isTrue(formatEquals, "Only %s format is allowed".formatted(format));

	}

	/**
	 * Row mapper which transforms single BigQuery row into a desired type.
	 * @param rowMapper your row mapper
	 */
	public void setRowMapper(final Converter<T, byte[]> rowMapper) {
		this.rowMapper = rowMapper;
	}

	private byte[] mapItemToCsv(final T t) {
		try {
			return rowMapper == null ? objectWriter.writeValueAsBytes(t) : rowMapper.convert(t);
		}
		catch (JacksonException e) {
			logger.error("Error during processing of the line: ", e);
			return new byte[] {};
		}
	}

}