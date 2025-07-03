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
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.BigQueryWriteApiPendingJsonItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * A builder for {@link BigQueryWriteApiPendingJsonItemWriter}.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href=
 * "https://github.com/spring-projects/spring-batch-extensions/tree/main/spring-batch-bigquery/src/test/java/org/springframework/batch/extensions/bigquery/unit/writer/writeapi/json/builder/BigQueryWriteApiPendingJsonItemWriterBuilderTest.java">Examples</a>
 */
public class BigQueryWriteApiPendingJsonItemWriterBuilder<T> {

	private BigQueryWriteClient bigQueryWriteClient;

	private TableName tableName;

	private JsonObjectMarshaller<T> marshaller;

	private ApiFutureCallback<AppendRowsResponse> apiFutureCallback;

	private Executor executor;

	/**
	 * GRPC client that will be responsible for communication with BigQuery.
	 * @param bigQueryWriteClient a client
	 * @return {@link BigQueryWriteApiPendingJsonItemWriterBuilder}
	 * @see BigQueryWriteApiPendingJsonItemWriter#setBigQueryWriteClient(BigQueryWriteClient)
	 */
	public BigQueryWriteApiPendingJsonItemWriterBuilder<T> bigQueryWriteClient(
			final BigQueryWriteClient bigQueryWriteClient) {
		this.bigQueryWriteClient = bigQueryWriteClient;
		return this;
	}

	/**
	 * A table name along with a full path.
	 * @param tableName a name
	 * @return {@link BigQueryWriteApiPendingJsonItemWriterBuilder}
	 * @see BigQueryWriteApiPendingJsonItemWriter#setTableName(TableName)
	 */
	public BigQueryWriteApiPendingJsonItemWriterBuilder<T> tableName(final TableName tableName) {
		this.tableName = tableName;
		return this;
	}

	/**
	 * Converts your DTO into a {@link String}.
	 * @param marshaller your mapper
	 * @return {@link BigQueryWriteApiPendingJsonItemWriterBuilder}
	 * @see BigQueryWriteApiPendingJsonItemWriter#setMarshaller(JsonObjectMarshaller)
	 */
	public BigQueryWriteApiPendingJsonItemWriterBuilder<T> marshaller(final JsonObjectMarshaller<T> marshaller) {
		this.marshaller = marshaller;
		return this;
	}

	/**
	 * A {@link ApiFutureCallback} that will be called on successful or failed event.
	 * @param apiFutureCallback a callback
	 * @return {@link BigQueryWriteApiPendingJsonItemWriterBuilder}
	 * @see BigQueryWriteApiPendingJsonItemWriter#setApiFutureCallback(ApiFutureCallback)
	 */
	public BigQueryWriteApiPendingJsonItemWriterBuilder<T> apiFutureCallback(
			final ApiFutureCallback<AppendRowsResponse> apiFutureCallback) {
		this.apiFutureCallback = apiFutureCallback;
		return this;
	}

	/**
	 * {@link Executor} that will be used for {@link ApiFutureCallback}.
	 * @param executor an executor
	 * @return {@link BigQueryWriteApiPendingJsonItemWriterBuilder}
	 * @see BigQueryWriteApiPendingJsonItemWriter#setExecutor(Executor)
	 * @see BigQueryWriteApiPendingJsonItemWriter#setApiFutureCallback(ApiFutureCallback)
	 */
	public BigQueryWriteApiPendingJsonItemWriterBuilder<T> executor(final Executor executor) {
		this.executor = executor;
		return this;
	}

	/**
	 * Please remember about
	 * {@link BigQueryWriteApiPendingJsonItemWriter#afterPropertiesSet()}.
	 * @return {@link BigQueryWriteApiPendingJsonItemWriter}
	 * @throws IOException in case when {@link BigQueryWriteClient} failed to be created
	 * automatically
	 */
	public BigQueryWriteApiPendingJsonItemWriter<T> build() throws IOException {
		BigQueryWriteApiPendingJsonItemWriter<T> writer = new BigQueryWriteApiPendingJsonItemWriter<>();

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
