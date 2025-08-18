/*
 * Copyright 2025-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.extensions.excel.support.rowset;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.MockSheet;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultRowSetTests {

	private DefaultRowSet rowSet;

	@BeforeEach
	void setUp() {
		this.rowSet = new DefaultRowSet(new MockSheet(
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
		this.rowSet.next();
		var properties = this.rowSet.getProperties();

		assertThat(properties.size()).isEqualTo(2);
		assertThat(properties.getProperty("cola")).isEqualTo("col1a");
		assertThat(properties.getProperty("colb")).isEqualTo("col1b");
		assertThat(properties.getProperty("colc")).isNull();
	}
}
