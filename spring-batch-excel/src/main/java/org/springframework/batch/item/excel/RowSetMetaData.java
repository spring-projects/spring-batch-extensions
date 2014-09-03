package org.springframework.batch.item.excel;

/**
 * Created by in329dei on 3-9-2014.
 */
public class RowSetMetaData {

	private final Sheet sheet;

	RowSetMetaData(Sheet sheet) {
		this.sheet = sheet;
	}

	public String[] getColumnNames() {
		return sheet.getHeader();
	}

	public String getColumnName(int idx) {
		String[] names = getColumnNames();
		return names[idx];
	}

	public int getColumnCount() {
		return sheet.getNumberOfColumns();
	}

	public String getSheetName() {
		return sheet.getName();
	}

}
