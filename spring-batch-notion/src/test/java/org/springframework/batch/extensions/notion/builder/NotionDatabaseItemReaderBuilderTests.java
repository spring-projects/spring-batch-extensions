/*
 * Copyright 2024-2026 the original author or authors.
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
package org.springframework.batch.extensions.notion.builder;

import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.notion.Filter;
import org.springframework.batch.extensions.notion.NotionDatabaseItemReader;
import org.springframework.batch.extensions.notion.Sort;
import org.springframework.batch.extensions.notion.mapping.PropertyMapper;

import static org.assertj.core.api.BDDAssertions.catchException;
import static org.assertj.core.api.BDDAssertions.then;
import static org.springframework.util.ClassUtils.getShortName;

/**
 * @author Jaeung Ha
 */
class NotionDatabaseItemReaderBuilderTests {

	@Test
	void should_succeed() {
		// GIVEN
		String token = "TOKEN";
		String databaseId = "DATABASE ID";
		PropertyMapper<String> propertyMapper = properties -> "PROPERTY";
		Filter filter = Filter.where().checkbox("IsActive").isEqualTo(true);
		String name = "NAME";
		Sort[] sorts = new Sort[0];
		int pageSize = 50;
		boolean saveState = true;
		int maxItemCount = 1000;
		int currentItemCount = 10;
		String baseUrl = "https://example.com";

		NotionDatabaseItemReaderBuilder<String> underTest = new NotionDatabaseItemReaderBuilder<String>() //
			.token(token)
			.databaseId(databaseId)
			.propertyMapper(propertyMapper)
			.filter(filter)
			.name(name)
			.sorts(sorts)
			.pageSize(pageSize)
			.saveState(saveState)
			.maxItemCount(maxItemCount)
			.currentItemCount(currentItemCount)
			.baseUrl(baseUrl);

		// WHEN
		NotionDatabaseItemReader<String> reader = underTest.build();

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
	void should_fail_when_token_is_null() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> underTest = new NotionDatabaseItemReaderBuilder<>() //
			.token(null)
			.databaseId("DATABASE ID")
			.propertyMapper(properties -> "PROPERTY")
			.saveState(false);

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("token, databaseId, and propertyMapper must not be null");
	}

	@Test
	void should_fail_when_databaseId_is_null() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> underTest = new NotionDatabaseItemReaderBuilder<>().token("TOKEN")
			.databaseId(null)
			.propertyMapper(properties -> "PROPERTY")
			.saveState(false);

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("token, databaseId, and propertyMapper must not be null");
	}

	@Test
	void should_fail_when_propertyMapper_is_null() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<Object> underTest = new NotionDatabaseItemReaderBuilder<>().token("TOKEN")
			.databaseId("DATABASE ID")
			.propertyMapper(null)
			.saveState(false);

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("token, databaseId, and propertyMapper must not be null");
	}

	@Test
	void should_succeed_when_saveState_is_false_and_name_is_not_set() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<String> underTest = new NotionDatabaseItemReaderBuilder<String>() //
			.token("TOKEN")
			.propertyMapper(properties -> "PROPERTY")
			.databaseId("DATABASE ID")
			.saveState(false);

		// WHEN
		NotionDatabaseItemReader<String> reader = underTest.build();

		// THEN
		then(reader).extracting("saveState").isEqualTo(false);
		then(reader).extracting("name").isEqualTo(getShortName(NotionDatabaseItemReader.class));
	}

	@Test
	void should_fail_when_saveState_is_true_and_name_is_blank() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<String> underTest = new NotionDatabaseItemReaderBuilder<String>() //
			.token("TOKEN")
			.propertyMapper(properties -> "PROPERTY")
			.databaseId("DATABASE ID")
			.saveState(true)
			.name("");

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("A name is required when saveState is set to true.");
	}

	@Test
	void should_fail_when_pageSize_is_greater_than_100() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<String> underTest = new NotionDatabaseItemReaderBuilder<String>() //
			.token("TOKEN")
			.propertyMapper(properties -> "PROPERTY")
			.databaseId("DATABASE ID")
			.pageSize(101);

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("pageSize must be less than or equal to 100");
	}

	@Test
	void should_fail_when_pageSize_is_less_than_zero() {
		// GIVEN
		NotionDatabaseItemReaderBuilder<String> underTest = new NotionDatabaseItemReaderBuilder<String>() //
			.token("TOKEN")
			.propertyMapper(properties -> "PROPERTY")
			.databaseId("DATABASE ID")
			.pageSize(-1);

		// WHEN
		Exception exception = catchException(underTest::build);

		// THEN
		then(exception) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("pageSize must be greater than zero");
	}

}
