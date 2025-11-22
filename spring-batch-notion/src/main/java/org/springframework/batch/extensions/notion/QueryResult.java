package org.springframework.batch.extensions.notion;

import tools.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.List;

@JsonNaming(SnakeCaseStrategy.class)
record QueryResult(List<Page> results, String nextCursor, boolean hasMore) {

}
