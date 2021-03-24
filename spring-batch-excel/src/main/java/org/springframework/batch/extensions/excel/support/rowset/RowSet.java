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

import java.util.Properties;

/**
 * Used by the {@code org.springframework.batch.item.excel.AbstractExcelItemReader} to
 * abstract away the complexities of the underlying Excel API implementations.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public interface RowSet {

	/**
	 * Retrieves the meta data (name of the sheet, number of columns, names) of this row
	 * set.
	 * @return a corresponding {@code RowSetMetaData} instance.
	 */
	RowSetMetaData getMetaData();

	/**
	 * Move to the next row in the document.
	 * @return {@code true} if the row is valid, {@code false} if there are no more rows
	 */
	boolean next();

	/**
	 * Returns the current row number.
	 * @return the current row number
	 */
	int getCurrentRowIndex();

	/**
	 * Return the current row as a {@code String[]}.
	 * @return the row as a {@code String[]}
	 */
	String[] getCurrentRow();

	/**
	 * Construct name-value pairs from the column names and string values. {@code null}
	 * values are omitted.
	 * @return some properties representing the row set.
	 * @throws IllegalStateException if the column name meta data is not available.
	 */
	Properties getProperties();

}
