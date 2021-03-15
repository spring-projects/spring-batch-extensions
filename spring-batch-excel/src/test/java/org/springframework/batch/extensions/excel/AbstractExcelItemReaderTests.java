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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Base class for testing Excel based item readers.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public abstract class AbstractExcelItemReaderTests {

	protected final Log logger = LogFactory.getLog(this.getClass());

	protected AbstractExcelItemReader<String[]> itemReader;

	@BeforeEach
	public void setup() throws Exception {
		this.itemReader = createExcelItemReader();
		this.itemReader.setLinesToSkip(1); // First line is column names
		this.itemReader.setResource(new ClassPathResource("player.xls"));
		this.itemReader.setRowMapper(new PassThroughRowMapper());
		this.itemReader.setSkippedRowsCallback((rs) -> this.logger.info("Skipping: " + Arrays.toString(rs.getCurrentRow())));
		configureItemReader(this.itemReader);
		this.itemReader.afterPropertiesSet();
		ExecutionContext executionContext = new ExecutionContext();
		this.itemReader.open(executionContext);
	}

	protected void configureItemReader(AbstractExcelItemReader<String[]> itemReader) {
	}

	@AfterEach
	public void after() {
		this.itemReader.close();
	}

	@Test
	public void readExcelFile() throws Exception {
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

}
