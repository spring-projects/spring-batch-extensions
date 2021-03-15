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

package org.springframework.batch.extensions.excel;

import java.util.Iterator;
import java.util.List;

/**
 * Sheet implementation usable for testing. Works in an {@code List} of {@xode String[]}.
 *
 * @author Marten Deinum
 * @since 0.1.0
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
		return this.rows.size();
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public String[] getRow(int rowNumber) {
		if (rowNumber < getNumberOfRows()) {
			return this.rows.get(rowNumber);
		}
		else {
			return null;
		}
	}

	@Override
	public Iterator<String[]> iterator() {
		return this.rows.iterator();
	}

}
