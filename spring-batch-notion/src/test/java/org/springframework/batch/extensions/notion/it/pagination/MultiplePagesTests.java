/*
 * Copyright 2024-2025 the original author or authors.
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
package org.springframework.batch.extensions.notion.it.pagination;

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
import static org.springframework.batch.extensions.notion.it.ResponseBodies.queryResponse;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.result;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.richText;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.title;

/**
 * @author Stefano Cordio
 */
@IntegrationTest
class MultiplePagesTests {

	private static final UUID DATABASE_ID = randomUUID();

	private static final int PAGE_SIZE = 2;

	@Autowired
	JobOperatorTestUtils jobOperator;

	@Autowired
	ListItemWriter<PaginatedJob.Item> itemWriter;

	@Test
	void should_succeed() throws Exception {
		// GIVEN
		UUID thirdResultId = randomUUID();

		JSONObject firstResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("Another name string"), "Value", richText("0987654321")));
		JSONObject secondResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("Name string"), "Value", richText("123456")));
		JSONObject thirdResult = result(thirdResultId, DATABASE_ID,
				Map.of("Name", title(""), "Value", richText("abc-1234")));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(PAGE_SIZE)))
			.willReturn(okJson(queryResponse(thirdResultId, firstResult, secondResult))));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(thirdResultId, PAGE_SIZE)))
			.willReturn(okJson(queryResponse(thirdResult))));

		// WHEN
		JobExecution jobExecution = jobOperator.startJob();

		// THEN
		then(jobExecution.getExitStatus()).isEqualTo(COMPLETED);

		then(itemWriter.getWrittenItems()).asInstanceOf(LIST)
			.containsExactly( //
					new PaginatedJob.Item("Another name string", "0987654321"), //
					new PaginatedJob.Item("Name string", "123456"), //
					new PaginatedJob.Item("", "abc-1234"));
	}

	@SpringBootApplication
	static class PaginatedJob {

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
			NotionDatabaseItemReader<Item> reader = new NotionDatabaseItemReader<>();

			reader.setSaveState(false);

			reader.setToken("token");
			reader.setBaseUrl(wiremockBaseUrl);
			reader.setDatabaseId(DATABASE_ID.toString());

			reader.setPageSize(PAGE_SIZE);
			reader.setPropertyMapper(new RecordPropertyMapper<>());

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
