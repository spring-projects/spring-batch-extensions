package org.springframework.batch.extensions.notion;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.BDDAssertions.then;

class RichTextTests {

	private final JsonMapper jsonMapper = new JsonMapper();

	@Test
	void fromJson() {
		// GIVEN
		String json = """
				{
				  "type": "text",
				  "text": {
				    "content": "Some words ",
				    "link": null
				  },
				  "annotations": {
				    "bold": false,
				    "italic": false,
				    "strikethrough": false,
				    "underline": false,
				    "code": false,
				    "color": "default"
				  },
				  "plain_text": "Some words ",
				  "href": null
				}
				""";
		RichText expected = new RichText("Some words ");
		// WHEN
		RichText result = jsonMapper.readValue(json, RichText.class);
		// THEN
		then(result).isEqualTo(expected);
	}

}
