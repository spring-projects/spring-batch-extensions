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

import java.util.Collections;
import java.util.List;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * @author Marten Deinum
 * @since 0.1.0
 */
public class MockExcelItemReader<T> extends AbstractExcelItemReader<T> {

	private final List<MockSheet> sheets;

	public MockExcelItemReader(MockSheet sheet) {
		this(Collections.singletonList(sheet));
	}

	public MockExcelItemReader(List<MockSheet> sheets) {
		this.sheets = sheets;
		super.setResource(new ByteArrayResource(new byte[0]));
	}

	@Override
	protected Sheet getSheet(int sheet) {
		return this.sheets.get(sheet);
	}

	@Override
	protected int getNumberOfSheets() {
		return this.sheets.size();
	}

	@Override
	protected void openExcelFile(Resource resource, String password) throws Exception {

	}

	@Override
	protected void doClose() throws Exception {
		this.sheets.clear();
	}

}
