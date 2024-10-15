/*
 * Copyright 2002-2024 the original author or authors.
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

import org.springframework.batch.extensions.notion.NotionDatabaseItemReader;
import org.springframework.batch.extensions.notion.Sort;
import org.springframework.batch.extensions.notion.it.IntegrationTest;
import org.springframework.batch.extensions.notion.mapping.RecordPropertyMapper;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.givenThat;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.springframework.batch.extensions.notion.Sort.Direction.DESCENDING;
import static org.springframework.batch.extensions.notion.it.RequestBodies.queryRequest;
import static org.springframework.batch.extensions.notion.it.RequestBodies.sortByProperty;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION_VALUE;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.queryResponse;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.result;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.richText;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.title;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.springframework.batch.core.ExitStatus.COMPLETED;
import static wiremock.com.google.common.net.HttpHeaders.AUTHORIZATION;
import static wiremock.com.google.common.net.HttpHeaders.CONTENT_TYPE;

/**
 * @author Stefano Cordio
 */
@IntegrationTest
class MultiplePagesDescendingTests {

	private static final UUID DATABASE_ID = randomUUID();

	private static final int PAGE_SIZE = 2;

	@Autowired
	JobLauncherTestUtils launcher;

	@Autowired
	ListItemWriter<PaginatedDescendingJob.Item> itemWriter;

	@Test
	void should_succeed() throws Exception {
		// GIVEN
		UUID thirdResultId = randomUUID();

		JSONObject firstResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("Name string"), "Value", richText("123456")));
		JSONObject secondResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("Another name string"), "Value", richText("0987654321")));
		JSONObject thirdResult = result(thirdResultId, DATABASE_ID,
				Map.of("Name", title(""), "Value", richText("abc-1234")));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(PAGE_SIZE, sortByProperty("Name", DESCENDING))))
			.willReturn(okJson(queryResponse(thirdResultId, firstResult, secondResult))));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)) //
			.withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(thirdResultId, PAGE_SIZE, sortByProperty("Name", DESCENDING))))
			.willReturn(okJson(queryResponse(thirdResult))));

		// WHEN
		JobExecution jobExecution = launcher.launchJob();

		// THEN
		then(jobExecution.getExitStatus()).isEqualTo(COMPLETED);

		then(itemWriter.getWrittenItems()).asInstanceOf(LIST)
			.containsExactly( //
					new PaginatedDescendingJob.Item("Name string", "123456"), //
					new PaginatedDescendingJob.Item("Another name string", "0987654321"), //
					new PaginatedDescendingJob.Item("", "abc-1234"));
	}

	@SpringBootApplication
	static class PaginatedDescendingJob {

		@Value("${wiremock.server.baseUrl}")
		private String wiremockBaseUrl;

		@Bean
		Job job(JobRepository jobRepository, Step step) {
			return new JobBuilder("TEST-JOB", jobRepository).start(step).build();
		}

		@Bean
		Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
			return new StepBuilder("TEST-STEP", jobRepository) //
				.<Item, Item>chunk(PAGE_SIZE, transactionManager) //
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
			reader.setSorts(Sort.by("Name", DESCENDING));
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
