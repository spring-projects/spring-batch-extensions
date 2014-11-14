/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel.support.rowset;

import java.util.Properties;

/**
 * Used by the {@code org.springframework.batch.item.excel.AbstractExcelItemReader} to abstract away
 * the complexities of the underlying Excel API implementations.
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
public interface RowSet {

    /**
     * Retrieves the meta data (name of the sheet, number of columns, names) of this row set.
     *
     * @return a corresponding {@code RowSetMetaData} instance.
     */
    RowSetMetaData getMetaData();


    /**
     * Move to the next row in the document.
     *
     * @return true if the row is valid, false if there are no more rows
     */
    boolean next();

    /**
     * Returns the current row number
     *
     * @return the current row number
     */
    int getCurrentRowIndex();

    /**
     * Return the current row as a String[].
     *
     * @return the row as a String[]
     */
    String[] getCurrentRow();

    /**
     * Retrieves the value of the indicated column in the current row as a String object.
     *
     * @param idx the column index, 0 based
     * @return a String objeect respresenting the column value.
     */
    String getColumnValue(int idx);


    /**
     * Construct name-value pairs from the column names and string values. Null
     * values are omitted.
     *
     * @return some properties representing the row set.
     * @throws IllegalStateException if the column name meta data is not
     *                               available.
     */
    Properties getProperties();
}
