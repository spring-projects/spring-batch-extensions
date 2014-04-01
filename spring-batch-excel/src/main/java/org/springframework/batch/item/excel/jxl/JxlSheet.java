
/*
 * Copyright 2014 the original author or authors.
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
 */
public class JxlSheet implements Sheet {

    private final jxl.Sheet delegate;

    /**
     * Constructor which takes the delegate sheet.
     * 
     * @param delegate the JXL sheet
     */
    JxlSheet(final jxl.Sheet delegate) {
        super();
        this.delegate = delegate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfRows() {
        return this.delegate.getRows();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getHeader() {
        return this.getRow(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getRow(final int rowNumber) {
        final Cell[] row = this.delegate.getRow(rowNumber);
        return JxlUtils.extractContents(row);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.delegate.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfColumns() {
        return this.delegate.getColumns();
    }

}
