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
package org.springframework.batch.extensions.notion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.CREATED_TIME;

/**
 * @author Stefano Cordio
 */
class QueryRequestTests {

	private final JsonMapper jsonMapper = new JsonMapper();

	@ParameterizedTest
	@FieldSource
	void toJson(QueryRequest underTest, String expected) throws Exception {
		// WHEN
		String result = jsonMapper.writeValueAsString(underTest);
		// THEN
		assertEquals(expected, result, true);
	}

	static List<Arguments> toJson = List.of( //
			arguments(new QueryRequest(42, null, null), """
					{
					  "page_size" : 42
					}
					"""), //
			arguments(new QueryRequest(42, "cursor", null), """
					{
					  "page_size" : 42,
					  "start_cursor" : "cursor"
					}
					"""), //
			arguments(new QueryRequest(42, null, null, Sort.by("property")), """
					{
					  "page_size" : 42,
					  "sorts" : [
					    {
					      "direction" : "ascending",
					      "property" : "property"
					    }
					  ]
					}
					"""), //
			arguments(new QueryRequest(42, null, null, Sort.by("property"), Sort.by(CREATED_TIME)), """
					{
					  "page_size" : 42,
					  "sorts" : [
					    {
					      "property" : "property",
					      "direction" : "ascending"
					    },
					    {
					      "timestamp" : "created_time",
					      "direction" : "ascending"
					    }
					  ]
					}
					"""));

}
