package org.springframework.batch.extensions.notion;

import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.notion.PageProperty.RichTextProperty;
import org.springframework.batch.extensions.notion.PageProperty.TitleProperty;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;

class PagePropertyTests {

	private final JsonMapper jsonMapper = new JsonMapper();

	@Test
	void richTextProperty() {
		// GIVEN
		String json = """
				{
				  "Description": {
				    "id": "HbZT",
				    "type": "rich_text",
				    "rich_text": [
				      {
				        "type": "text",
				        "text": {
				          "content": "There is some ",
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
				        "plain_text": "There is some ",
				        "href": null
				      },
				      {
				        "type": "text",
				        "text": {
				          "content": "text",
				          "link": null
				        },
				        "annotations": {
				          "bold": true,
				          "italic": false,
				          "strikethrough": false,
				          "underline": false,
				          "code": false,
				          "color": "default"
				        },
				        "plain_text": "text",
				        "href": null
				      },
				      {
				        "type": "text",
				        "text": {
				          "content": " in this property!",
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
				        "plain_text": " in this property!",
				        "href": null
				      }
				    ]
				  }
				}
				""";
		Map<String, RichTextProperty> expected = Map.of("Description", new RichTextProperty(List.of( //
				new RichText("There is some "), //
				new RichText("text"), //
				new RichText(" in this property!"))));
		// WHEN
		Map<String, RichTextProperty> result = jsonMapper.readValue(json, new TypeReference<>() {
		});
		// THEN
		then(result).isEqualTo(expected);
	}

	@Test
	void titleProperty() {
		// GIVEN
		String json = """
				{
				  "Title": {
				    "id": "title",
				    "type": "title",
				    "title": [
				      {
				        "type": "text",
				        "text": {
				          "content": "A better title for the page",
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
				        "plain_text": "This is also not done",
				        "href": null
				      }
				    ]
				  }
				}
				""";
		Map<String, TitleProperty> expected = Map.of("Title",
				new TitleProperty(List.of(new RichText("This is also not done"))));
		// WHEN
		Map<String, TitleProperty> result = jsonMapper.readValue(json, new TypeReference<>() {
		});
		// THEN
		then(result).isEqualTo(expected);
	}

}
