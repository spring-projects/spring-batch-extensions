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

package org.springframework.batch.extensions.excel.streaming;

import java.util.Locale;

import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.excel.Player;
import org.springframework.batch.extensions.excel.ReflectionTestUtils;
import org.springframework.batch.extensions.excel.mapping.BeanWrapperRowMapper;
import org.springframework.batch.extensions.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.extensions.excel.support.rowset.StaticColumnNameExtractor;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingXlsxMappingTests {

	@Test
	void readAndMapRowsUsingRowMapper() throws Exception {
		var columns = new String[] {"id", "position", "lastName", "firstName", "birthYear", "debutYear"};
		var rowSetFactory = new DefaultRowSetFactory();
		rowSetFactory.setColumnNameExtractor(new StaticColumnNameExtractor(columns));

		var mapper = new BeanWrapperRowMapper<Player>();
		mapper.setTargetType(Player.class);

		var reader = new StreamingXlsxItemReader<Player>();
		reader.setResource(new ClassPathResource("player.xlsx"));
		reader.setRowSetFactory(rowSetFactory);
		reader.setRowMapper(mapper);
		reader.setLinesToSkip(1); // Skip header
		reader.setUserLocale(Locale.US); // Use a Locale to not be dependent on environment
		reader.afterPropertiesSet();

		reader.open(new ExecutionContext());
		Player row;
		do {
			row = reader.read();
		}
		while (row != null);

		Integer readCount = (Integer) ReflectionTestUtils.getField(reader, "currentItemCount");
		assertThat(readCount).isEqualTo(4321);
	}
}
