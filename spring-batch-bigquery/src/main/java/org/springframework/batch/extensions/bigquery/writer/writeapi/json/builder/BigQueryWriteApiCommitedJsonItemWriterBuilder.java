/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.writer.writeapi.json.builder;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.BigQueryWriteApiCommitedJsonItemWriter;
import org.springframework.batch.infrastructure.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.infrastructure.item.json.JsonObjectMarshaller;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * A builder for {@link BigQueryWriteApiCommitedJsonItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href=
 * "https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/writeapi/json/builder/BigQueryWriteApiCommitedJsonItemWriterBuilderTest.java">Examples</a>
 */
public class BigQueryWriteApiCommitedJsonItemWriterBuilder<T> {

	private BigQueryWriteClient bigQueryWriteClient;

	private TableName tableName;

	private JsonObjectMarshaller<T> marshaller;

	private ApiFutureCallback<AppendRowsResponse> apiFutureCallback;

	private Executor executor;

	/**
	 * Default constructor
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder() {
	}

	/**
	 * GRPC client that will be responsible for communication with BigQuery.
	 * @param bigQueryWriteClient a client
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriterBuilder}
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setBigQueryWriteClient(BigQueryWriteClient)
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder<T> bigQueryWriteClient(
			final BigQueryWriteClient bigQueryWriteClient) {
		this.bigQueryWriteClient = bigQueryWriteClient;
		return this;
	}

	/**
	 * A table name along with a full path.
	 * @param tableName a name
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriterBuilder}
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setTableName(TableName)
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder<T> tableName(final TableName tableName) {
		this.tableName = tableName;
		return this;
	}

	/**
	 * Converts your DTO into a {@link String}.
	 * @param marshaller your mapper
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriterBuilder}
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setMarshaller(JsonObjectMarshaller)
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder<T> marshaller(final JsonObjectMarshaller<T> marshaller) {
		this.marshaller = marshaller;
		return this;
	}

	/**
	 * A {@link ApiFutureCallback} that will be called on successful or failed event.
	 * @param apiFutureCallback a callback
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriterBuilder}
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setApiFutureCallback(ApiFutureCallback)
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder<T> apiFutureCallback(
			final ApiFutureCallback<AppendRowsResponse> apiFutureCallback) {
		this.apiFutureCallback = apiFutureCallback;
		return this;
	}

	/**
	 * {@link Executor} that will be used for {@link ApiFutureCallback}.
	 * @param executor an executor
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriterBuilder}
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setExecutor(Executor)
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setApiFutureCallback(ApiFutureCallback)
	 */
	public BigQueryWriteApiCommitedJsonItemWriterBuilder<T> executor(final Executor executor) {
		this.executor = executor;
		return this;
	}

	/**
	 * Please remember about
	 * {@link BigQueryWriteApiCommitedJsonItemWriter#afterPropertiesSet()}.
	 * @return {@link BigQueryWriteApiCommitedJsonItemWriter}
	 * @throws IOException in case when {@link BigQueryWriteClient} failed to be created
	 * automatically
	 */
	public BigQueryWriteApiCommitedJsonItemWriter<T> build() throws IOException {
		final BigQueryWriteApiCommitedJsonItemWriter<T> writer = new BigQueryWriteApiCommitedJsonItemWriter<>();

		writer.setMarshaller(this.marshaller == null ? new JacksonJsonObjectMarshaller<>() : this.marshaller);

		writer.setBigQueryWriteClient(
				this.bigQueryWriteClient == null ? BigQueryWriteClient.create() : this.bigQueryWriteClient);

		if (apiFutureCallback != null) {
			writer.setApiFutureCallback(apiFutureCallback);
			writer.setExecutor(this.executor == null ? MoreExecutors.directExecutor() : this.executor);
		}

		writer.setTableName(tableName);

		return writer;
	}

}
