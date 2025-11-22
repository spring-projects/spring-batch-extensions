package org.springframework.batch.extensions.notion;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonTypeName;
import tools.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.List;

@JsonTypeInfo(use = Id.NAME, property = "type")
interface PageProperty {

	@JsonTypeName("rich_text")
	@JsonNaming(SnakeCaseStrategy.class)
	record RichTextProperty(List<RichText> richText) implements PageProperty {
	}

	@JsonTypeName("title")
	record TitleProperty(List<RichText> title) implements PageProperty {
	}

}
