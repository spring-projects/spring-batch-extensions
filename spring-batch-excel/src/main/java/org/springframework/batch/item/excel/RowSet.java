package org.springframework.batch.item.excel;

import java.util.Properties;

/**
 * @author Marten Deinum
 */
public class RowSet {

	private final Sheet sheet;
	private final RowSetMetaData metaData;

	private int currentRowIndex = -1;
	private String[] currentRow;

	public RowSet(Sheet sheet) {
		this.sheet=sheet;
		this.metaData = new RowSetMetaData(sheet);
	}

	public RowSetMetaData getMetaData() {
		return metaData;
	}

	public boolean next() {
		currentRow = null;
		currentRowIndex++;
		if (currentRowIndex <= sheet.getNumberOfRows()) {
			currentRow = sheet.getRow(currentRowIndex);
			return true;
		}
		return false;
	}

	/**
     * The current row index represents the number of rows
	 * into a {@link Sheet} the current line resides
	 */
 	public int getCurrentRowIndex() {
		return this.currentRowIndex;
	}

	/**
	 * Get the data of the current row.
	 *
	 * @return a String[] for the current data
	 */
	public String[] getCurrentRow() {
		return this.currentRow;
	}

	/**
	 * Get the value of the given column
	 *
	 * @param idx index of the column to get, 0 based
	 * @return the value
	 * @throws java.lang.ArrayIndexOutOfBoundsException
	 */
	public String getColumnValue(int idx) {
		return currentRow[idx];
	}

	/**
	 * Construct name-value pairs from the column names and string values. Null
	 * values are omitted.
	 *
	 * @return some properties representing the row set.
	 *
	 * @throws IllegalStateException if the column name meta data is not
	 * available.
	 */
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
