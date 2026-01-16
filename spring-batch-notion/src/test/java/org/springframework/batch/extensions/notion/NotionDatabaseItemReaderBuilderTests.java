/*
 * Copyright 2024-2025 the original author or authors.
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

package org.springframework.batch.extensions.notion;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;
import org.springframework.test.util.ReflectionTestUtils;

class NotionDatabaseItemReaderBuilderTests {

	@Test
	void should_succeed() {
		// GIVEN
		String expectedToken = "FOO TOKEN";
		String expectedDatabaseId = "FOO DATABASE ID";
		PropertyMapper<String> expectedPropertyMapper = properties -> "FOO PROPERTY";
		Filter expectedFilter = Filter.where().checkbox("IsActive").isEqualTo(true);
		String expectedName = "FOO NAME";
		Sort[] expectedSorts = new Sort[0];
		int expectedPageSize = 50;
		boolean expectedSaveState = true;
		int expectedMaxItemCount = 1000;
		int expectedCurrentItemCount = 10;
		String expectedBaseUrl = "https://example.com";

		// WHEN
		NotionDatabaseItemReader<String> reader = new NotionDatabaseItemReaderBuilder<String>().token(expectedToken)
			.databaseId(expectedDatabaseId)
			.propertyMapper(expectedPropertyMapper)
			.filter(expectedFilter)
			.name(expectedName)
			.sorts(expectedSorts)
			.pageSize(expectedPageSize)
			.saveState(expectedSaveState)
			.maxItemCount(expectedMaxItemCount)
			.currentItemCount(expectedCurrentItemCount)
			.baseUrl(expectedBaseUrl)
			.build();

		// THEN
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "token")).isEqualTo(expectedToken);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "databaseId")).isEqualTo(expectedDatabaseId);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "propertyMapper")).isEqualTo(expectedPropertyMapper);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "filter")).isEqualTo(expectedFilter);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "sorts")).isEqualTo(expectedSorts);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "pageSize")).isEqualTo(expectedPageSize);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "saveState")).isEqualTo(expectedSaveState);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "name")).isEqualTo(expectedName);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "maxItemCount")).isEqualTo(expectedMaxItemCount);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "currentItemCount"))
			.isEqualTo(expectedCurrentItemCount);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "baseUrl")).isEqualTo(expectedBaseUrl);
	}

	@Test
	void should_succeed_when_saveStateIsFalse_and_nameIsNull() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		boolean saveState = false;
		String name = null;

		NotionDatabaseItemReaderBuilder<String> builder = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.propertyMapper(propertyMapper)
			.databaseId(databaseId)
			.saveState(saveState)
			.name(name);

		// WHEN
		NotionDatabaseItemReader<String> reader = builder.build();

		// THEN
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "saveState")).isEqualTo(false);
		Assertions.assertThat(ReflectionTestUtils.getField(reader, "name")).isNull();
	}

	@Test
	void should_fail_when_tokenIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token(null)
			.databaseId("FOO DATABASE ID")
			.propertyMapper(properties -> "FOO PROPERTY");

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
	}

	@Test
	void should_fail_when_databaseIdIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token("FOO TOKEN")
			.databaseId(null)
			.propertyMapper(properties -> "FOO PROPERTY");

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
	}

	@Test
	void should_fail_when_propertyMapperIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token("FOO TOKEN")
			.databaseId("FOO DATABASE ID")
			.propertyMapper(null);

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build).isInstanceOf(NullPointerException.class);
	}

	@Test
	void should_fail_when_saveStateIsTrue_and_nameIsBlank() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		boolean saveState = true;
		String name = "";

		NotionDatabaseItemReaderBuilder<String> builder = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.propertyMapper(propertyMapper)
			.databaseId(databaseId)
			.saveState(saveState)
			.name(name);

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build)
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("A name is required when saveState is set to true.");
	}

	@Test
	void should_fail_when_saveStateIsTrue_and_nameIsNull() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		boolean saveState = true;
		String name = null;

		NotionDatabaseItemReaderBuilder<String> builder = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.propertyMapper(propertyMapper)
			.databaseId(databaseId)
			.saveState(saveState)
			.name(name);

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build)
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("A name is required when saveState is set to true.");
	}

	@Test
	void should_fail_when_pageSizeIsGreaterThan100() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		String name = "FOO NAME";
		int pageSize = 101;

		NotionDatabaseItemReaderBuilder<String> builder = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.propertyMapper(propertyMapper)
			.databaseId(databaseId)
			.name(name)
			.pageSize(pageSize);

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build)
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("pageSize must be less than or equal to 100");
	}

	@Test
	void should_fail_when_pageSizeIsSmallerThanZero() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		String name = "FOO NAME";
		int pageSize = -1;

		NotionDatabaseItemReaderBuilder<String> builder = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.propertyMapper(propertyMapper)
			.databaseId(databaseId)
			.name(name)
			.pageSize(pageSize);

		// WHEN & THEN
		Assertions.assertThatThrownBy(builder::build)
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("pageSize must be greater than zero");
	}

}
