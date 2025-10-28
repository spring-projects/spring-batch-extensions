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

/**
 * Interface representing the the metadata associated with an Excel document.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public interface RowSetMetaData {

	/**
	 * Retrieves the names of the columns for the current sheet.
	 * @return the column names.
	 */
	String[] getColumnNames();

	/**
	 * Retrieves the name of the sheet the RowSet is based on.
	 * @return the name of the sheet
	 */
	String getSheetName();

	/**
	 * Retrieves the number of available rows for the current sheet.
	 * <p><strong>Note:</strong> The result might be indeterministic depending on the
	 * {@code Sheet} implementation used in the {@code RowSetMetaData} implementation.
	 * Some implementations may return an estimate or cached value rather than the exact count.
	 * @return total rows
	 */
	int getRowsCount();
}
