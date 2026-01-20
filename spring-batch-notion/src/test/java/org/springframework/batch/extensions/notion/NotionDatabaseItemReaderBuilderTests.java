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

import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;

import static org.assertj.core.api.BDDAssertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.then;

class NotionDatabaseItemReaderBuilderTests {

	@Test
	void should_succeed() {
		// GIVEN
		String token = "FOO TOKEN";
		String databaseId = "FOO DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "FOO PROPERTY";
		Filter filter = Filter.where().checkbox("IsActive").isEqualTo(true);
		String name = "FOO NAME";
		Sort[] sorts = new Sort[0];
		int pageSize = 50;
		boolean saveState = true;
		int maxItemCount = 1000;
		int currentItemCount = 10;
		String baseUrl = "https://example.com";

		// WHEN
		NotionDatabaseItemReader<String> reader = new NotionDatabaseItemReaderBuilder<String>().token(token)
			.databaseId(databaseId)
			.propertyMapper(propertyMapper)
			.filter(filter)
			.name(name)
			.sorts(sorts)
			.pageSize(pageSize)
			.saveState(saveState)
			.maxItemCount(maxItemCount)
			.currentItemCount(currentItemCount)
			.baseUrl(baseUrl)
			.build();

		// THEN
		then(reader).extracting("token").isEqualTo(token);
		then(reader).extracting("databaseId").isEqualTo(databaseId);
		then(reader).extracting("propertyMapper").isEqualTo(propertyMapper);
		then(reader).extracting("filter").isEqualTo(filter);
		then(reader).extracting("sorts").isEqualTo(sorts);
		then(reader).extracting("pageSize").isEqualTo(pageSize);
		then(reader).extracting("saveState").isEqualTo(saveState);
		then(reader).extracting("name").isEqualTo(name);
		then(reader).extracting("maxItemCount").isEqualTo(maxItemCount);
		then(reader).extracting("currentItemCount").isEqualTo(currentItemCount);
		then(reader).extracting("baseUrl").isEqualTo(baseUrl);
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
		then(reader).extracting("saveState").isEqualTo(false);
		then(reader).extracting("name").isEqualTo(NotionDatabaseItemReader.class.getSimpleName());
	}

	@Test
	void should_fail_when_tokenIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token(null)
			.databaseId("FOO DATABASE ID")
			.propertyMapper(properties -> "FOO PROPERTY")
			.saveState(false);

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class);
		then(exception).hasMessage("token, databaseId, and propertyMapper must not be null");
	}

	@Test
	void should_fail_when_databaseIdIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token("FOO TOKEN")
			.databaseId(null)
			.propertyMapper(properties -> "FOO PROPERTY")
			.saveState(false);

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class);
		then(exception).hasMessage("token, databaseId, and propertyMapper must not be null");
	}

	@Test
	void should_fail_when_propertyMapperIsNull() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> builder = new NotionDatabaseItemReaderBuilder<>().token("FOO TOKEN")
			.databaseId("FOO DATABASE ID")
			.propertyMapper(null)
			.saveState(false);

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class);
		then(exception).hasMessage("token, databaseId, and propertyMapper must not be null");
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

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class)
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

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class)
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

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class)
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

		// WHEN
		Throwable exception = catchThrowable(builder::build);

		// THEN
		then(exception).isInstanceOf(IllegalArgumentException.class).hasMessage("pageSize must be greater than zero");
	}

}
