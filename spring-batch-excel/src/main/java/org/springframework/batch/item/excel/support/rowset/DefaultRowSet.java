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

import org.springframework.batch.item.excel.Sheet;

import java.util.Properties;

/**
 * Default implementation of the {@code RowSet} interface.
 *
 * @author Marten Deinum
 * @since 0.5.0
 *
 * @see org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory
 */
public class DefaultRowSet implements RowSet {

    private final Sheet sheet;
    private final RowSetMetaData metaData;

    private int currentRowIndex = -1;
    private String[] currentRow;

    DefaultRowSet(Sheet sheet, RowSetMetaData metaData) {
        this.sheet = sheet;
        this.metaData = metaData;
    }

    @Override
    public RowSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public boolean next() {
        currentRow = null;
        currentRowIndex++;
        if (currentRowIndex < sheet.getNumberOfRows()) {
            currentRow = sheet.getRow(currentRowIndex);
            return true;
        }
        return false;
    }

    @Override
    public int getCurrentRowIndex() {
        return this.currentRowIndex;
    }

    @Override
    public String[] getCurrentRow() {
        return this.currentRow;
    }

    @Override
    public String getColumnValue(int idx) {
        return currentRow[idx];
    }

    @Override
    public Properties getProperties() {
        final String[] names = metaData.getColumnNames();
        if (names == null) {
            throw new IllegalStateException("Cannot create properties without meta data");
        }

        Properties props = new Properties();
        for (int i = 0; i < currentRow.length; i++) {
            String value = currentRow[i];
            if (value != null) {
                props.setProperty(names[i], value);
            }
        }
        return props;
    }
}
