/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.batch.extensions.excel;

import java.util.Arrays;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.batch.extensions.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.DefaultResourceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Base class for testing Excel based item readers.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractExcelItemReaderTests {

	protected static final Consumer<AbstractExcelItemReader<String[]>> NOOP = (reader) -> { };

	private final DefaultResourceLoader resourceLoader = new DefaultResourceLoader();

	protected final Log logger = LogFactory.getLog(this.getClass());

	private AbstractExcelItemReader<String[]> itemReader;

	@BeforeEach
	public void setup() {
		this.itemReader = createExcelItemReader();
		this.itemReader.setLinesToSkip(1); // First line is column names
		this.itemReader.setRowMapper(new PassThroughRowMapper());
		this.itemReader.setSkippedRowsCallback((rs) -> this.logger.info("Skipping: " + Arrays.toString(rs.getCurrentRow())));
		this.itemReader.setUserLocale(Locale.US); // Fixed Locale to prevent changes in different environments

	}

	protected void configureAndOpenItemReader(String resource, Consumer<AbstractExcelItemReader<String[]>> configurer) {
		this.itemReader.setResource(this.resourceLoader.getResource(resource));
		configurer.accept(this.itemReader);
		this.itemReader.afterPropertiesSet();

		ExecutionContext executionContext = new ExecutionContext();
		this.itemReader.open(executionContext);
	}


	@AfterEach
	public void after() {
		this.itemReader.close();
	}

	@ParameterizedTest
	@MethodSource("scenarios")
	public void readExcelFile(String resource, Consumer<AbstractExcelItemReader<String[]>> configurer) throws Exception {
		configureAndOpenItemReader(resource, configurer);
		assertThat(this.itemReader.getNumberOfSheets()).isEqualTo(3);
		String[] row;
		do {
			row = this.itemReader.read();
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("Read: " + Arrays.toString(row));
			}
			if (row != null) {
				assertThat(row).hasSize(6);
			}
		}
		while (row != null);
		Integer readCount = (Integer) ReflectionTestUtils.getField(this.itemReader, "currentItemCount");
		assertThat(readCount).isEqualTo(4321);
	}


	@Test
	public void testRequiredProperties() {
		assertThatThrownBy(() -> {
			final AbstractExcelItemReader<String[]> reader = createExcelItemReader();
			reader.afterPropertiesSet();
		}).isInstanceOf(IllegalArgumentException.class);
	}

	protected abstract AbstractExcelItemReader<String[]> createExcelItemReader();

	protected abstract Stream<Arguments> scenarios();

}
