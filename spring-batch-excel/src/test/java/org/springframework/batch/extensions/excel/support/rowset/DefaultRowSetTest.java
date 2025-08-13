package org.springframework.batch.extensions.excel.support.rowset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.batch.extensions.excel.MockSheet;
import org.springframework.batch.extensions.excel.Sheet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;

class DefaultRowSetTest {

	private DefaultRowSet rowSet;

	@BeforeEach
	void setUp() {
		rowSet = new DefaultRowSet(new MockSheet(
				"Sheet1",
				Arrays.asList("col1a,col1b,col1c".split(","), "col2a,col2b,col2c".split(","), "col3a,col3b,col3c".split(","))
		), new RowSetMetaData() {
			@Override
			public String[] getColumnNames() {
				return new String[]{ "cola", "colb"};
			}

			@Override
			public String getSheetName() {
				return "Sheet1";
			}
		});
	}

	@Test
	void shouldReturnPropsSizeEqualsToMetadataColumns() {
		rowSet.next();
		var properties = rowSet.getProperties();

		assertThat(properties.size()).isEqualTo(2);
		assertThat(properties.getProperty("cola")).isEqualTo("col1a");
		assertThat(properties.getProperty("colb")).isEqualTo("col1b");
		assertThat(properties.getProperty("colc")).isNull();
	}
}
