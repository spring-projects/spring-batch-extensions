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
import org.springframework.batch.extensions.notion.it.datasource.AutomaticDataSourceDiscoveryTest.AutomaticDiscoveryJob.Item;
import org.springframework.batch.extensions.notion.mapping.RecordPropertyMapper;
import org.springframework.batch.infrastructure.item.support.ListItemWriter;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.common.ContentTypes.AUTHORIZATION;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.springframework.batch.core.ExitStatus.COMPLETED;
import static org.springframework.batch.extensions.notion.it.RequestBodies.queryRequest;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION_VALUE;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.*;

/**
 * Tests for {@link NotionDatabaseItemReader} with automatic data source discovery.
 *
 * @author Roeniss Moon
 */
@IntegrationTest
class AutomaticDataSourceDiscoveryTest {

	private static final UUID DATABASE_ID = randomUUID();

	private static final UUID FIRST_DATA_SOURCE_ID = randomUUID();

	private static final UUID SECOND_DATA_SOURCE_ID = randomUUID();

	private static final int PAGE_SIZE = 2;

	@Autowired
	JobOperatorTestUtils jobOperator;

	@Autowired
	ListItemWriter<Item> itemWriter;

	@Test
	void should_use_firstly_returned_data_source_if_not_specified() throws Exception {
		// GIVEN
		JSONObject firstResult = result(randomUUID(), FIRST_DATA_SOURCE_ID,
				Map.of("Name", title("From first source"), "Value", richText("First value")));

		// Database API returns 2 data sources (1st: default, 2nd: secondary)
		// Discovery should automatically use the 1st data source
		givenThat(get("/databases/%s".formatted(DATABASE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.willReturn(okJson(databaseInfoResponse(DATABASE_ID, FIRST_DATA_SOURCE_ID, SECOND_DATA_SOURCE_ID))));

		// Query should go to the 1st data source (discovered automatically)
		givenThat(post("/data_sources/%s/query".formatted(FIRST_DATA_SOURCE_ID)) //
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
			.containsExactly(new Item("From first source", "First value"));
	}

	@SpringBootApplication
	static class AutomaticDiscoveryJob {

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
			// Use 3-parameter constructor - data source ID will be discovered
			// automatically
			NotionDatabaseItemReader<Item> reader = new NotionDatabaseItemReader<>("token", DATABASE_ID.toString(),
					new RecordPropertyMapper<>());

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
