package org.springframework.batch.extensions.notion;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import tools.jackson.databind.annotation.JsonNaming;

import java.util.List;

@JsonNaming(SnakeCaseStrategy.class)
@JsonInclude(Include.NON_EMPTY)
record QueryRequest(int pageSize, @Nullable String startCursor, @Nullable Filter filter, Sort... sorts) {
}
