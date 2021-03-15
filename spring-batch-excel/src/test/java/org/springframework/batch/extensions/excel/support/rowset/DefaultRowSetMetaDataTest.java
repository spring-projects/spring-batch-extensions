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

package org.springframework.batch.extensions.excel.support.rowset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.batch.extensions.excel.Sheet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for {@link DefaultRowSetMetaData}
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public class DefaultRowSetMetaDataTest {

	private static final String[] COLUMNS = { "col1", "col2", "col3" };

	private DefaultRowSetMetaData rowSetMetaData;

	private Sheet sheet;

	private ColumnNameExtractor columnNameExtractor;

	@BeforeEach
	public void setup() {
		this.sheet = Mockito.mock(Sheet.class);
		this.columnNameExtractor = Mockito.mock(ColumnNameExtractor.class);
		this.rowSetMetaData = new DefaultRowSetMetaData(this.sheet, this.columnNameExtractor);
	}

	@Test
	public void shouldReturnColumnsFromColumnNameExtractor() {

		given(this.columnNameExtractor.getColumnNames(this.sheet)).willReturn(COLUMNS);

		String[] names = this.rowSetMetaData.getColumnNames();

		assertThat(names).isEqualTo(new String[] { "col1", "col2", "col3" });

		verify(this.columnNameExtractor, times(1)).getColumnNames(this.sheet);
		verifyNoMoreInteractions(this.sheet, this.columnNameExtractor);
	}

	@Test
	public void shouldGetAndReturnNameOfTheSheet() {

		given(this.sheet.getName()).willReturn("testing123");

		String name = this.rowSetMetaData.getSheetName();

		assertThat(name).isEqualTo("testing123");

		verify(this.sheet, times(1)).getName();
		verifyNoMoreInteractions(this.sheet);
	}

}
