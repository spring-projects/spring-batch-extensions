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

package org.springframework.batch.extensions.excel.poi;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.FormulaError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.ReflectionTestUtils;
import org.springframework.batch.extensions.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
public class PoiItemReaderWithErrorsTest {

	private final Log logger = LogFactory.getLog(this.getClass());

	private PoiItemReader<String[]> itemReader;

	@BeforeEach
	public void setup() throws Exception {
		this.itemReader = new PoiItemReader<>();
		this.itemReader.setResource(new ClassPathResource("errors.xlsx"));
		this.itemReader.setLinesToSkip(1); // First line is column names
		this.itemReader.setRowMapper(new PassThroughRowMapper());
		this.itemReader.setSkippedRowsCallback((rs) -> this.logger.info("Skipping: " + Arrays.toString(rs.getCurrentRow())));
		this.itemReader.afterPropertiesSet();

		ExecutionContext executionContext = new ExecutionContext();
		this.itemReader.open(executionContext);
	}

	@Test
	public void readExcelFileWithBlankRow() throws Exception {
		assertThat(this.itemReader.getNumberOfSheets()).isEqualTo(1);
		String[] row;
		String[] lastRow = null;
		do {
			row = this.itemReader.read();
			this.logger.debug("Read: " + Arrays.toString(row));
			if (row != null) {
				lastRow = row;
				assertThat(row).hasSize(3);
			}
		}
		while (row != null);
		Integer readCount = (Integer) ReflectionTestUtils.getField(this.itemReader, "currentItemCount");
		assertThat(readCount).isEqualTo(3);
		assertThat(lastRow[2]).isEqualTo(FormulaError.DIV0.getString());

	}

}
