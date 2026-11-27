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

package org.springframework.batch.extensions.notion.it.restartability;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.extensions.notion.NotionDatabaseItemReader;
import org.springframework.batch.extensions.notion.it.IntegrationTest;
import org.springframework.batch.extensions.notion.mapping.RecordPropertyMapper;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.support.ListItemWriter;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.givenThat;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.springframework.batch.extensions.notion.it.RequestBodies.queryRequest;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION;
import static org.springframework.batch.extensions.notion.it.RequestHeaders.NOTION_VERSION_VALUE;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.queryResponse;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.result;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.richText;
import static org.springframework.batch.extensions.notion.it.ResponseBodies.title;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.http.HttpHeaders.CONTENT_TYPE;

/**
 * @author Hwang In Gyu
 */
@IntegrationTest
class RestartabilityTests {

	private static final UUID DATABASE_ID = randomUUID();

	private static final int PAGE_SIZE = 2;

	@Autowired
	JobOperatorTestUtils jobOperator;

	@Autowired
	JobRepository jobRepository;

	@Autowired
	FailingOnceListItemWriter<Item> itemWriter;

	@Test
	void should_restart_from_last_committed_chunk() throws Exception {
		// GIVEN
		UUID nextCursor = randomUUID();

		JSONObject firstResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("first-page-item-1"), "Value", richText("100")));
		JSONObject secondResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("first-page-item-2"), "Value", richText("200")));
		JSONObject thirdResult = result(randomUUID(), DATABASE_ID,
				Map.of("Name", title("second-page-item-1"), "Value", richText("300")));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)).withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(PAGE_SIZE)))
			.willReturn(okJson(queryResponse(nextCursor, firstResult, secondResult))));

		givenThat(post("/databases/%s/query".formatted(DATABASE_ID)).withHeader(AUTHORIZATION, matching("Bearer .+"))
			.withHeader(CONTENT_TYPE, containing("application/json"))
			.withHeader(NOTION_VERSION, equalTo(NOTION_VERSION_VALUE))
			.withRequestBody(equalToJson(queryRequest(nextCursor, PAGE_SIZE)))
			.willReturn(okJson(queryResponse(thirdResult))));

		// WHEN
		JobExecution firstExecution = jobOperator.startJob();

		// THEN
		then(firstExecution.getStatus()).isEqualTo(BatchStatus.FAILED);
		then(itemWriter.getWrittenItems()).asInstanceOf(LIST)
			.containsExactly(new Item("first-page-item-1", "100"), new Item("first-page-item-2", "200"));

		// WHEN
		JobExecution restartedExecution = jobOperator.getJobOperator().restart(firstExecution);
		JobExecution refreshedExecution = jobRepository.getJobExecution(restartedExecution.getId());

		// THEN
		then(refreshedExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
		then(itemWriter.getWrittenItems()).asInstanceOf(LIST)
			.containsExactly(new Item("first-page-item-1", "100"), new Item("first-page-item-2", "200"),
					new Item("second-page-item-1", "300"));
	}

	@SpringBootApplication
	@EnableBatchProcessing
	@EnableJdbcJobRepository(dataSourceRef = "batchDataSource", transactionManagerRef = "batchTransactionManager")
	static class RestartableCursorJob {

		@Value("${wiremock.server.baseUrl}")
		private String wiremockBaseUrl;

		@Bean
		DataSource batchDataSource() {
			return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
				.addScript("org/springframework/batch/core/schema-h2.sql")
				.build();
		}

		@Bean
		PlatformTransactionManager batchTransactionManager(DataSource batchDataSource) {
			return new JdbcTransactionManager(batchDataSource);
		}

		@Bean
		Job job(JobRepository jobRepository, Step step) {
			return new JobBuilder("job", jobRepository).start(step).build();
		}

		@Bean
		Step step(JobRepository jobRepository, PlatformTransactionManager batchTransactionManager) {
			return new StepBuilder("step", jobRepository).<Item, Item>chunk(PAGE_SIZE)
				.transactionManager(batchTransactionManager)
				.reader(itemReader())
				.writer(itemWriter())
				.build();
		}

		@Bean
		NotionDatabaseItemReader<Item> itemReader() {
			NotionDatabaseItemReader<Item> reader = new NotionDatabaseItemReader<>("token", DATABASE_ID.toString(),
					new RecordPropertyMapper<>());

			reader.setBaseUrl(wiremockBaseUrl);
			reader.setPageSize(PAGE_SIZE);
			return reader;
		}

		@Bean
		FailingOnceListItemWriter<Item> itemWriter() {
			return new FailingOnceListItemWriter<>();
		}

	}

	record Item(String name, String value) {
	}

	static class FailingOnceListItemWriter<T> extends ListItemWriter<T> {

		private final AtomicBoolean failedOnce = new AtomicBoolean(false);

		@Override
		public void write(Chunk<? extends T> chunk) throws Exception {
			if (!chunk.isEmpty() && chunk.size() == 1 && this.failedOnce.compareAndSet(false, true)) {
				throw new IllegalStateException("Planned failure for restart test");
			}
			super.write(chunk);
		}

	}

}
