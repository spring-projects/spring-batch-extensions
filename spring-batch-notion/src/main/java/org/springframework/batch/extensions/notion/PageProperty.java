package org.springframework.batch.extensions.notion;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({ //
		@JsonSubTypes.Type(name = "rich_text", value = PageProperty.RichTextProperty.class),
		@JsonSubTypes.Type(name = "title", value = PageProperty.TitleProperty.class) //
})
interface PageProperty {

	@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
	record RichTextProperty(List<RichText> richText) implements PageProperty {
	}

	record TitleProperty(List<RichText> title) implements PageProperty {
	}

}
