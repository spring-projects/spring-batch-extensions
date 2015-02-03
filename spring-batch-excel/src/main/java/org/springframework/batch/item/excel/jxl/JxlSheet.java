
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

package org.springframework.batch.item.excel.jxl;

import jxl.Cell;
import org.springframework.batch.item.excel.Sheet;

/**
 * {@link org.springframework.batch.item.excel.Sheet} implementation for JXL.
 *
 * @author Marten Deinum
 * @since 0.5.0
 * @deprecated since JExcelAPI is an abandoned project (no release since 2009, with serious bugs remaining)
 */
@Deprecated
public class JxlSheet implements Sheet {

    private final jxl.Sheet delegate;
    private final int numberOfRows;
    private final int numberOfColumns;
    private final String name;

    /**
     * Constructor which takes the delegate sheet.
     *
     * @param delegate the JXL sheet
     */
    JxlSheet(final jxl.Sheet delegate) {
        super();
        this.delegate = delegate;
        this.numberOfRows = this.delegate.getRows();
        this.numberOfColumns = this.delegate.getNumberOfImages();
        this.name=this.delegate.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfRows() {
        return this.numberOfRows;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getRow(final int rowNumber) {
        if (rowNumber < getNumberOfRows()) {
            final Cell[] row = this.delegate.getRow(rowNumber);
            return JxlUtils.extractContents(row);
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfColumns() {
        return this.numberOfColumns;
    }

}
