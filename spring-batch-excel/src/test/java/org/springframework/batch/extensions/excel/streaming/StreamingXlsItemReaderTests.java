/*
 * Copyright 2002-2026 the original author or authors.
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

import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import org.springframework.batch.extensions.excel.AbstractExcelItemReader;
import org.springframework.batch.extensions.excel.AbstractExcelItemReaderTests;

/**
 * @author Kebba Manneh
 * @since 0.3.0
 */
class StreamingXlsItemReaderTests extends AbstractExcelItemReaderTests {

	@Override
	protected AbstractExcelItemReader<String[]> createExcelItemReader() {
		return new StreamingXlsItemReader<>();
	}

	@Override
	protected Stream<Arguments> scenarios() {
		return Stream.of(
				Arguments.of("classpath:/player.xls", NOOP),
				Arguments.of("classpath:/player_with_blank_lines.xls", NOOP));
	}
}
