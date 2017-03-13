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

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;

/**
 * Sheet implementation for Apache POI representing each row as a String[].
 *
 * @author Marten Deinum
 * @author Mattias Jiderhamn
 * @since 0.5.0
 */
public class StringArrayPoiSheet extends PoiSheet<String[]> {

    /**
     * Constructor which takes the delegate sheet.
     *
     * @param delegate the apache POI sheet
     */
    public StringArrayPoiSheet(final org.apache.poi.ss.usermodel.Sheet delegate) {
        super(delegate);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getRow(final int rowNumber) {
        final Row row = this.delegate.getRow(rowNumber);
        if (row == null) {
            return null;
        }
        final List<String> cells = new LinkedList<String>();

        for (int i = 0; i < getNumberOfColumns(); i++) {
            Cell cell = row.getCell(i);
            switch (cell.getCellType()) {
                case Cell.CELL_TYPE_NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        Date date = cell.getDateCellValue();
                        cells.add(String.valueOf(date.getTime()));
                    } else {
                        cells.add(String.valueOf(cell.getNumericCellValue()));
                    }
                    break;
                case Cell.CELL_TYPE_BOOLEAN:
                    cells.add(String.valueOf(cell.getBooleanCellValue()));
                    break;
                case Cell.CELL_TYPE_STRING:
                case Cell.CELL_TYPE_BLANK:
                    cells.add(cell.getStringCellValue());
                    break;
                case Cell.CELL_TYPE_FORMULA:
                    cells.add(getFormulaEvaluator().evaluate(cell).formatAsString());
                    break;
                default:
                    throw new IllegalArgumentException("Cannot handle cells of type " + cell.getCellType());
            }
        }
        return cells.toArray(new String[cells.size()]);
    }
}
