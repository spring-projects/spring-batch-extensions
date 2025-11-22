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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.springframework.batch.extensions.notion.Sort.Direction.ASCENDING;
import static org.springframework.batch.extensions.notion.Sort.Direction.DESCENDING;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.CREATED_TIME;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.LAST_EDITED_TIME;

/**
 * @author Stefano Cordio
 */
class SortTests {

	private final JsonMapper jsonMapper = new JsonMapper();

	@ParameterizedTest
	@FieldSource
	void toJson(Sort underTest, String expected) throws Exception {
		// WHEN
		String result = jsonMapper.writeValueAsString(underTest);
		// THEN
		assertEquals(expected, result, true);
	}

	static List<Arguments> toJson = List.of( //
			arguments(Sort.by("property"), """
					{
					  "property" : "property",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by("property", ASCENDING), """
					{
					  "property" : "property",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by("property", DESCENDING), """
					{
					  "property" : "property",
					  "direction" : "descending"
					}
					"""), //
			arguments(Sort.by(CREATED_TIME), """
					{
					  "timestamp" : "created_time",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by(CREATED_TIME, ASCENDING), """
					{
					  "timestamp" : "created_time",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by(CREATED_TIME, DESCENDING), """
					{
					  "timestamp" : "created_time",
					  "direction" : "descending"
					}
					"""), //
			arguments(Sort.by(LAST_EDITED_TIME), """
					{
					  "timestamp" : "last_edited_time",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by(LAST_EDITED_TIME, ASCENDING), """
					{
					  "timestamp" : "last_edited_time",
					  "direction" : "ascending"
					}
					"""), //
			arguments(Sort.by(LAST_EDITED_TIME, DESCENDING), """
					{
					  "timestamp" : "last_edited_time",
					  "direction" : "descending"
					}
					"""));

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
