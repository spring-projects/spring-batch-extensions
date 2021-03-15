/*
 * Copyright 2006-2021 the original author or authors.
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

import org.springframework.batch.extensions.excel.Sheet;

/**
 * Default implementation for the {@code RowSetMetaData} interface.
 *
 * Requires a {@code Sheet} and {@code ColumnNameExtractor} to operate correctly.
 * Delegates the retrieval of the column names to the {@code ColumnNameExtractor}.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public class DefaultRowSetMetaData implements RowSetMetaData {

	private final Sheet sheet;

	private final ColumnNameExtractor columnNameExtractor;

	private String[] columnNames;

	DefaultRowSetMetaData(Sheet sheet, ColumnNameExtractor columnNameExtractor) {
		this.sheet = sheet;
		this.columnNameExtractor = columnNameExtractor;
	}

	@Override
	public String[] getColumnNames() {
		if (this.columnNames == null) {
			this.columnNames = this.columnNameExtractor.getColumnNames(this.sheet);
		}
		return this.columnNames;
	}

	@Override
	public String getSheetName() {
		return this.sheet.getName();
	}

}
