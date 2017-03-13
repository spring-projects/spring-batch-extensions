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

import java.util.Properties;

import org.springframework.batch.item.excel.Sheet;

/**
 * Default implementation of the {@code RowSet} interface.
 *
 * @author Marten Deinum
 * @since 0.5.0
 *
 * @see org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory
 */
public class DefaultRowSet extends AbstractRowSet<String[]> {

    DefaultRowSet(Sheet<String[]> sheet, RowSetMetaData metaData) {
        super(sheet, metaData);
    }

    @Override
    public String getColumnValue(int idx) {
        return getCurrentRow()[idx];
    }

    @Override
    public Properties getProperties() {
        final String[] names = getMetaData().getColumnNames();
        if (names == null) {
            throw new IllegalStateException("Cannot create properties without meta data");
        }

        Properties props = new Properties();
        final String[] currentRow = getCurrentRow();
        for (int i = 0; i < currentRow.length; i++) {
            String value = currentRow[i];
            if (value != null) {
                props.setProperty(names[i], value);
            }
        }
        return props;
    }
}
