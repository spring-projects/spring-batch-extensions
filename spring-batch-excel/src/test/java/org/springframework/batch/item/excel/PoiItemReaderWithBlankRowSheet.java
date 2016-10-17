package org.springframework.batch.item.excel;

/**
 * Created by Krishna Mishra on 10/17/2016.
 */

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.item.excel.poi.PoiItemReader;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.StringUtils;

/**
 * Created by mishrk3 on 3/17/2016.
 */
public class PoiItemReaderWithBlankRowSheet {

	protected final Log logger = LogFactory.getLog(this.getClass());

	protected AbstractExcelItemReader itemReader;

	private ExecutionContext executionContext;

	@Before
	public void setup() throws Exception {
		this.itemReader = createExcelItemReader();
		this.itemReader.setLinesToSkip(1); // First line is column names
		this.itemReader.setRowMapper(new PassThroughRowMapper());
		this.itemReader.setSkippedRowsCallback(new RowCallbackHandler() {

			public void handleRow(RowSet rs) {
				logger.info("Skipping: " + StringUtils.arrayToCommaDelimitedString(rs.getCurrentRow()));
			}
		});
		configureItemReaderWithBlankRowSheet(this.itemReader);
		this.itemReader.afterPropertiesSet();
		executionContext = new ExecutionContext();
		this.itemReader.open(executionContext);
	}

	@Test
	public void readExcelFileWithBlankRow() throws Exception {
		assertEquals(1, this.itemReader.getNumberOfSheets());
		String[] row;
		do {
			row = (String[]) this.itemReader.read();
			this.logger.debug("Read: " + StringUtils.arrayToCommaDelimitedString(row));
			if (row != null) {
				assertEquals(4, row.length);
			}
		} while (row != null);
		int readCount = (Integer) ReflectionTestUtils.getField(this.itemReader, "currentItemCount");
		assertEquals(7, readCount);
	}

	protected AbstractExcelItemReader createExcelItemReader() {
		return new PoiItemReader();
	}

	protected void configureItemReaderWithBlankRowSheet(AbstractExcelItemReader itemReader) {
		itemReader.setResource(new ClassPathResource("org/springframework/batch/item/excel/blankRow.xlsx"));
	}
}
