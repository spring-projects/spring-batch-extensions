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
package org.springframework.batch.extensions.notion.it.datasource;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.extensions.notion.NotionDatabaseItemReader;
import org.springframework.batch.extensions.notion.it.IntegrationTest;
import org.springframework.batch.extensions.notion.it.datasource.ManualDataSourceSelectionTests.ManualDataSourceJob.Item;
import org.springframework.batch.extensions.notion.mapping.RecordPropertyMapper;
import org.springframework.batch.infrastructure.item.support.ListItemWriter;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.givenThat;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.common.ContentTypes.AUTHORIZATION;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.springframework.batch.core.ExitStatus.COMPLETED;
import static org.springframework.batch.extensions.notion.it.RequestBodies.queryRequest;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION_VALUE;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.datasourceQueryResponse;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.result;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.richText;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.title;

/**
 * Tests for {@link NotionDatabaseItemReader} with manual data source ID specification.
 *
 * @author Roeniss Moon
 */
@IntegrationTest
class ManualDataSourceSelectionTests {

	private static final UUID DATABASE_ID = randomUUID();

	private static final UUID FIRST_DATA_SOURCE_ID = randomUUID();

	private static final UUID SECOND_DATA_SOURCE_ID = randomUUID();

	private static final int PAGE_SIZE = 2;

	@Autowired
	JobOperatorTestUtils jobOperator;

	@Autowired
	ListItemWriter<Item> itemWriter;

	@Test
	void should_use_manually_specified_data_source_id() throws Exception {
		// GIVEN
		JSONObject firstResult = result(randomUUID(), SECOND_DATA_SOURCE_ID,
				Map.of("Name", title("From second source"), "Value", richText("Second value")));

		// No GET /databases/{id} stub - discovery should be bypassed when dataSourceId is
		// provided.
		// If discovery is incorrectly called, the test will fail with 404.

		// Query should go directly to the 2nd data source (bypassing discovery)
		givenThat(post("/data_sources/%s/query".formatted(SECOND_DATA_SOURCE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(PAGE_SIZE)))
			.willReturn(okJson(datasourceQueryResponse(firstResult))));

		// WHEN
		JobExecution jobExecution = jobOperator.startJob();

		// THEN
		then(jobExecution.getExitStatus()).isEqualTo(COMPLETED);

		then(itemWriter.getWrittenItems()).asInstanceOf(LIST)
			.containsExactly(new Item("From second source", "Second value"));
	}

	@SpringBootApplication
	static class ManualDataSourceJob {

		@Value("${wiremock.server.baseUrl}")
		private String wiremockBaseUrl;

		@Bean
		Job job(JobRepository jobRepository, Step step) {
			return new JobBuilder(jobRepository).start(step).build();
		}

		@Bean
		Step step(JobRepository jobRepository) {
			return new StepBuilder(jobRepository) //
				.<Item, Item>chunk(PAGE_SIZE) //
				.reader(itemReader()) //
				.writer(itemWriter()) //
				.build();
		}

		@Bean
		NotionDatabaseItemReader<Item> itemReader() {
			// Use 4-parameter constructor with manual data source ID (2nd source)
			NotionDatabaseItemReader<Item> reader = new NotionDatabaseItemReader<>("token", DATABASE_ID.toString(),
					SECOND_DATA_SOURCE_ID.toString(), new RecordPropertyMapper<>());

			reader.setSaveState(false);
			reader.setBaseUrl(wiremockBaseUrl);
			reader.setPageSize(PAGE_SIZE);

			return reader;
		}

		@Bean
		ListItemWriter<Item> itemWriter() {
			return new ListItemWriter<>();
		}

		record Item(String name, String value) {
		}

	}

}
