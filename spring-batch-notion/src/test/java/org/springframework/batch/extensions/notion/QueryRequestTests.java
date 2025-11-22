package org.springframework.batch.extensions.notion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.springframework.batch.extensions.notion.Sort.Timestamp.CREATED_TIME;

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

	static List<?> toJson = List.of( //
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
