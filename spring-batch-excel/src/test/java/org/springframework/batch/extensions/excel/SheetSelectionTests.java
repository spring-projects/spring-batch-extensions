/*
 * Copyright 2026 the original author or authors.
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

package org.springframework.batch.extensions.excel;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.assertj.core.api.Assertions.assertThat;

class SheetSelectionTests {

	@Test
	void readsAllSheetsByDefault() throws Exception {
		assertThat(read(reader())).containsExactly("first", "second", "third");
	}

	@Test
	void readsSelectedSheetsInWorkbookOrder() throws Exception {
		MockExcelItemReader<String[]> reader = reader();
		reader.setSheetNames("third", "first");
		assertThat(read(reader)).containsExactly("first", "third");
	}

	@Test
	void readsNoRowsWhenNoSheetMatches() throws Exception {
		MockExcelItemReader<String[]> reader = reader();
		reader.setSheetNames("missing");
		assertThat(read(reader)).isEmpty();
	}

	private MockExcelItemReader<String[]> reader() {
		List<MockSheet> sheets = new ArrayList<>(List.of(sheet("first"), sheet("second"), sheet("third")));
		MockExcelItemReader<String[]> reader = new MockExcelItemReader<>(sheets);
		reader.setRowMapper(new PassThroughRowMapper());
		reader.afterPropertiesSet();
		return reader;
	}

	private MockSheet sheet(String name) {
		return new MockSheet(name, List.<String[]>of(new String[] { name }));
	}

	private List<String> read(MockExcelItemReader<String[]> reader) throws Exception {
		List<String> rows = new ArrayList<>();
		reader.open(new ExecutionContext());
		try {
			String[] row;
			while ((row = reader.read()) != null) {
				rows.add(row[0]);
			}
			return rows;
		}
		finally {
			reader.close();
		}
	}

}
