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
package org.springframework.batch.extensions.notion;

import notion.api.v1.model.databases.query.sort.QuerySort;
import notion.api.v1.model.databases.query.sort.QuerySortDirection;
import notion.api.v1.model.databases.query.sort.QuerySortTimestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;

import java.util.List;

import static notion.api.v1.model.databases.query.sort.QuerySortDirection.Ascending;
import static notion.api.v1.model.databases.query.sort.QuerySortDirection.Descending;
import static notion.api.v1.model.databases.query.sort.QuerySortTimestamp.CreatedTime;
import static notion.api.v1.model.databases.query.sort.QuerySortTimestamp.LastEditedTime;
import static org.assertj.core.api.BDDAssertions.from;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.springframework.batch.extensions.notion.Sort.Direction.ASCENDING;
import static org.springframework.batch.extensions.notion.Sort.Direction.DESCENDING;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.CREATED_TIME;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.LAST_EDITED_TIME;

/**
 * @author Stefano Cordio
 */
class SortTests {

	@ParameterizedTest
	@FieldSource
	void toQuerySort(Sort underTest, String property, QuerySortTimestamp timestamp, QuerySortDirection direction) {
		// WHEN
		QuerySort result = underTest.toQuerySort();
		// THEN
		then(result) //
			.returns(direction, from(QuerySort::getDirection))
			.returns(property, from(QuerySort::getProperty))
			.returns(timestamp, from(QuerySort::getTimestamp));
	}

	static List<Arguments> toQuerySort = List.of( //
			arguments(Sort.by("property"), "property", null, Ascending),
			arguments(Sort.by("property", ASCENDING), "property", null, Ascending),
			arguments(Sort.by("property", DESCENDING), "property", null, Descending),
			arguments(Sort.by(CREATED_TIME), null, CreatedTime, Ascending),
			arguments(Sort.by(CREATED_TIME, ASCENDING), null, CreatedTime, Ascending),
			arguments(Sort.by(CREATED_TIME, DESCENDING), null, CreatedTime, Descending),
			arguments(Sort.by(LAST_EDITED_TIME), null, LastEditedTime, Ascending),
			arguments(Sort.by(LAST_EDITED_TIME, ASCENDING), null, LastEditedTime, Ascending),
			arguments(Sort.by(LAST_EDITED_TIME, DESCENDING), null, LastEditedTime, Descending));

	@ParameterizedTest
	@FieldSource
	void testToString(Sort underTest, String expected) {
		// WHEN
		String result = underTest.toString();
		// THEN
		then(result).isEqualTo(expected);
	}

	static List<Arguments> testToString = List.of( //
			arguments(Sort.by("property"), "property: ASCENDING"),
			arguments(Sort.by("property", ASCENDING), "property: ASCENDING"),
			arguments(Sort.by("property", DESCENDING), "property: DESCENDING"),
			arguments(Sort.by(CREATED_TIME), "CREATED_TIME: ASCENDING"),
			arguments(Sort.by(CREATED_TIME, ASCENDING), "CREATED_TIME: ASCENDING"),
			arguments(Sort.by(CREATED_TIME, DESCENDING), "CREATED_TIME: DESCENDING"),
			arguments(Sort.by(LAST_EDITED_TIME), "LAST_EDITED_TIME: ASCENDING"),
			arguments(Sort.by(LAST_EDITED_TIME, ASCENDING), "LAST_EDITED_TIME: ASCENDING"),
			arguments(Sort.by(LAST_EDITED_TIME, DESCENDING), "LAST_EDITED_TIME: DESCENDING"));

}
