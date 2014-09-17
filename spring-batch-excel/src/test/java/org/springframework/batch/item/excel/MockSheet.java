package org.springframework.batch.item.excel;

import jxl.Cell;
import org.springframework.batch.item.excel.jxl.JxlUtils;

import java.util.List;

/**
 * Sheet implementation usable for testing. Works in an {@code List} of {@xode String[]}.
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
public class MockSheet implements Sheet {

    private final List<String[]> rows;
    private final String name;

    public MockSheet(String name, List<String[]> rows) {
        this.name = name;
        this.rows = rows;
    }

    @Override
    public int getNumberOfRows() {
        return rows.size();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String[] getRow(int rowNumber) {
        if (rowNumber < getNumberOfRows()) {
            return this.rows.get(rowNumber);
        } else {
            return null;
        }
    }

    @Override
    public int getNumberOfColumns() {
        if (rows.isEmpty()) {
            return 0;
        }
        return rows.get(0).length;
    }
}
