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

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.springframework.batch.extensions.excel.AbstractExcelItemReader;
import org.springframework.batch.extensions.excel.AbstractExcelItemReaderTests;

public class PoiItemReaderTests extends AbstractExcelItemReaderTests {

	@Override
	protected AbstractExcelItemReader<String[]> createExcelItemReader() {
		return new PoiItemReader<>();
	}

	public Stream<Arguments> scenarios() {
		return Stream.of(
				Arguments.of("classpath:/player.xls", NOOP),
				Arguments.of("classpath:/player.xlsx", NOOP),
				Arguments.of("classpath:/player_with_blank_lines.xls", NOOP),
				Arguments.of("classpath:/player_with_blank_lines.xlsx", NOOP),
				Arguments.of("classpath:/player_with_password.xls", (Consumer<AbstractExcelItemReader<?>>) (reader) -> reader.setPassword("readme")));
	}
}
