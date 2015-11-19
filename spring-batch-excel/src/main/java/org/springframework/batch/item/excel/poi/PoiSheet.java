package org.springframework.batch.item.excel.poi;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.springframework.batch.item.excel.Sheet;

/**
 * Sheet implementation for Apache POI.
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
public class PoiSheet implements Sheet {

    private final org.apache.poi.ss.usermodel.Sheet delegate;
    private final int numberOfRows;
    private final String name;
    private final boolean useDataFormatter;
    
    private int numberOfColumns = -1;
    private FormulaEvaluator evaluator;

    /**
     * Constructor which takes the delegate sheet.
     *
     * @param delegate the Apache POI sheet
     */
    PoiSheet(final org.apache.poi.ss.usermodel.Sheet delegate) {
    	this(delegate, false);
    }
    /**
     * Constructor which takes the delegate sheet and a boolean to indicate 
     * to use the {@link DataFormatter} to read cells.
     *
     * @param delegate the Apache POI sheet
     * @param useDataFormatter choose to use the {@link DataFormatter} to read cells
     */
    PoiSheet(final org.apache.poi.ss.usermodel.Sheet delegate, boolean useDataFormatter) {
    	super();
    	this.delegate = delegate;
    	this.numberOfRows = this.delegate.getLastRowNum() + 1;
    	this.name=this.delegate.getSheetName();
    	this.useDataFormatter = useDataFormatter;
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
        
        for (int i = 0; i < getNumberOfColumns(rowNumber); i++) {
            Cell cell = row.getCell(i);
            
            if (useDataFormatter) {
            	DataFormatter df = new DataFormatter(Locale.getDefault());
            	cells.add(df.formatCellValue(cell));
            } else {
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
        }
        return cells.toArray(new String[cells.size()]);
    }

    private FormulaEvaluator getFormulaEvaluator() {
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
            numberOfColumns = getNumberOfColumns(0);
        }
        return numberOfColumns;
    }    

    private int getNumberOfColumns(int forRow) {
    	return this.delegate.getRow(forRow).getLastCellNum();
    }    
    
    
}