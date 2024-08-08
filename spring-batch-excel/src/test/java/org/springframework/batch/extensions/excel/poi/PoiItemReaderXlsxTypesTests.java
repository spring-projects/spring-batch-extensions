/*
 * Copyright 2002-2024 the original author or authors.
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

package org.springframework.batch.extensions.excel.poi;

import java.util.Locale;

import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.mapping.PassThroughRowMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

class PoiItemReaderXlsxTypesTests {

	@Test
	void shouldBeAbleToReadMultipleTypes() throws Exception {
		var reader = new PoiItemReader<String[]>();
		reader.setResource(new ClassPathResource("types.xlsx"));
		reader.setRowMapper(new PassThroughRowMapper());
		reader.setLinesToSkip(1); // Skip header
		reader.setUserLocale(Locale.US); // Use a Locale to not be dependent on environment
		reader.afterPropertiesSet();

		reader.open(new ExecutionContext());

		var row1 = reader.read();
		var row2 = reader.read();
		assertThat(row1).containsExactly("1", "1.0", "5/12/24", "13:14:55", "5/12/24 13:14", "hello world");
		assertThat(row2).containsExactly("2", "2.5", "8/8/23", "11:12:13", "8/8/23 11:12", "world hello");
	}

	@Test
	void shouldBeAbleToReadMultipleTypesWithDatesAsIso() throws Exception {
		var reader = new PoiItemReader<String[]>();
		reader.setResource(new ClassPathResource("types.xls"));
		reader.setRowMapper(new PassThroughRowMapper());
		reader.setLinesToSkip(1); // Skip header
		reader.setUserLocale(Locale.US); // Use a Locale to not be dependent on environment
		reader.setDatesAsIso(true);
		reader.afterPropertiesSet();

		reader.open(new ExecutionContext());

		var row1 = reader.read();
		var row2 = reader.read();
		assertThat(row1).containsExactly("1", "1.0", "2024-05-12", "13:14:55", "2024-05-12T13:14:55", "hello world");
		assertThat(row2).containsExactly("2", "2.5", "2023-08-08", "11:12:13", "2023-08-08T11:12:13", "world hello");
	}
}
