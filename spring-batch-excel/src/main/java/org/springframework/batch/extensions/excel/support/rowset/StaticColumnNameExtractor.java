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

import java.util.Arrays;

import org.springframework.batch.extensions.excel.Sheet;

/**
 * {@code ColumnNameExtractor} implementation which returns a preset String[] to use as
 * the column names. Useful for those situations in which an Excel file without a header
 * row is read
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public class StaticColumnNameExtractor implements ColumnNameExtractor {

	private final String[] columnNames;

	public StaticColumnNameExtractor(String[] columnNames) {
		this.columnNames = columnNames;
	}

	@Override
	public String[] getColumnNames(Sheet sheet) {
		return Arrays.copyOf(this.columnNames, this.columnNames.length);
	}

}
