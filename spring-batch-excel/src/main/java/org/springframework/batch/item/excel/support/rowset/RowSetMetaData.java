/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel.support.rowset;

/**
 * Interface representing the the metadata associated with an Excel document.
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
public interface RowSetMetaData {

    /**
     * Retrieves the names of the columns for the current sheet.
     *
     * @return the column names.
     */
    String[] getColumnNames();

    /**
     * Retrieves the column name for the indicatd column.
     *
     * @param idx the index of the column, 0 based
     * @return the column name
     */
    String getColumnName(int idx);

    /**
     * Retrieves the number of columns in the RowSet.
     *
     * @return the number of columns
     */
    int getColumnCount();

    /**
     * Retrieves the name of the sheet the RowSet is based on.
     *
     * @return the name of the sheet
     */
    String getSheetName();
}
