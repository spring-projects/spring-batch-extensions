/*
 * Copyright 2006-2017 the original author or authors.
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

/**
 * Abstract implementation of the {@code RowSet} interface, providing the ability to traverse rows.
 *
 * @param <R> Type used for representing a single row, such as an array
 * @author Marten Deinum
 * @author Mattias Jiderhamn
 * @since 0.5.0
 *
 * @see DefaultRowSetFactory
 */
public abstract class AbstractRowSet<R> implements RowSet<R> {

    private final Sheet<R> sheet;
    private final RowSetMetaData metaData;

    private int currentRowIndex = -1;
    private R currentRow;

    public AbstractRowSet(Sheet<R> sheet, RowSetMetaData metaData) {
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
    public R getCurrentRow() {
        return this.currentRow;
    }

}
