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

package org.springframework.batch.item.excel.poi;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.springframework.batch.item.excel.Sheet;

/**
 * Sheet implementation for Apache POI.
 *
 * @param <R> Type used for representing a single row, such as an array
 * @author Marten Deinum
 * @since 0.5.0
 */
public abstract class PoiSheet<R> implements Sheet<R> {

    protected final org.apache.poi.ss.usermodel.Sheet delegate;
    private final int numberOfRows;
    private final String name;

    private int numberOfColumns = -1;
    private FormulaEvaluator evaluator;

    /**
     * Constructor which takes the delegate sheet.
     *
     * @param delegate the apache POI sheet
     */
    public PoiSheet(final org.apache.poi.ss.usermodel.Sheet delegate) {
        super();
        this.delegate = delegate;
        this.numberOfRows = this.delegate.getLastRowNum() + 1;
        this.name=this.delegate.getSheetName();
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
    public String getName() {
        return this.name;
    }
    
    protected FormulaEvaluator getFormulaEvaluator() {
        if (this.evaluator == null) {
            this.evaluator = delegate.getWorkbook().getCreationHelper().createFormulaEvaluator();
        }
        return this.evaluator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfColumns() {
        if (numberOfColumns < 0) {
            numberOfColumns = this.delegate.getRow(0).getLastCellNum();
        }
        return numberOfColumns;
    }
}
