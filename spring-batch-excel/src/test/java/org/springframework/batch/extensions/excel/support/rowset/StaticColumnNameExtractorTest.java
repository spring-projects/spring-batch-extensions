/*
 * Copyright 2006-2021 the original author or authors.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
public class StaticColumnNameExtractorTest {

	private static final String[] COLUMNS = { "col1", "col2", "col3", "foo", "bar" };

	@Test
	public void shouldReturnSameHeadersAsPassedIn() {

		StaticColumnNameExtractor columnNameExtractor = new StaticColumnNameExtractor(COLUMNS);
		String[] names = columnNameExtractor.getColumnNames(null);
		assertThat(names)
				.isEqualTo(new String[] { "col1", "col2", "col3", "foo", "bar" })
				.isNotSameAs(COLUMNS);
	}

	@Test
	public void shouldReturnACopyOfTheHeaders() {

		StaticColumnNameExtractor columnNameExtractor = new StaticColumnNameExtractor(COLUMNS);
		String[] names = columnNameExtractor.getColumnNames(null);

		assertThat(names)
				.isEqualTo(new String[] { "col1", "col2", "col3", "foo", "bar" })
				.isNotSameAs(COLUMNS);
	}

}
