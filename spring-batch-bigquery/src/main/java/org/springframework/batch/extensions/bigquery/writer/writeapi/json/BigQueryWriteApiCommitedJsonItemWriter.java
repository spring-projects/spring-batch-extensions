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

package org.springframework.batch.extensions.bigquery.writer.writeapi.json;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.batch.extensions.bigquery.writer.BigQueryItemWriterException;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JSON writer for BigQuery using Storage Write API.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @see <a href="https://en.wikipedia.org/wiki/JSON">JSON</a>
 * @see <a href="https://cloud.google.com/bigquery/docs/write-api#committed_type">Commited
 * type storage write API</a>
 * @since 0.2.0
 */
public class BigQueryWriteApiCommitedJsonItemWriter<T> implements ItemWriter<T>, InitializingBean {

	/**
	 * Logger that can be reused
	 */
	private final Log logger = LogFactory.getLog(getClass());

	private final AtomicLong bigQueryWriteCounter = new AtomicLong();

	private BigQueryWriteClient bigQueryWriteClient;

	private TableName tableName;

	private JsonObjectMarshaller<T> marshaller;

	private ApiFutureCallback<AppendRowsResponse> apiFutureCallback;

	private Executor executor;

	private boolean writeFailed;

	@Override
	public void write(final Chunk<? extends T> chunk) throws Exception {
		if (!chunk.isEmpty()) {
			final List<? extends T> items = chunk.getItems();
			String streamName = null;

			try {
				WriteStream writeStreamToCreate = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();

				CreateWriteStreamRequest createStreamRequest = CreateWriteStreamRequest.newBuilder()
					.setParent(tableName.toString())
					.setWriteStream(writeStreamToCreate)
					.build();

				WriteStream writeStream = bigQueryWriteClient.createWriteStream(createStreamRequest);
				streamName = writeStream.getName();

				if (logger.isDebugEnabled()) {
					logger.debug("Created a stream=" + streamName);
				}

				final JsonStreamWriter jsonWriter = JsonStreamWriter
					.newBuilder(writeStream.getName(), bigQueryWriteClient)
					.build();

				try (jsonWriter) {
					if (logger.isDebugEnabled()) {
						logger.debug(String.format("Mapping %d elements", items.size()));
					}
					final JSONArray array = new JSONArray();
					items.stream().map(marshaller::marshal).map(JSONObject::new).forEach(array::put);

					if (logger.isDebugEnabled()) {
						logger.debug("Writing data to BigQuery");
					}
					final ApiFuture<AppendRowsResponse> future = jsonWriter.append(array);

					if (apiFutureCallback != null) {
						ApiFutures.addCallback(future, apiFutureCallback, executor);
					}
				}
			}
			catch (Exception e) {
				writeFailed = true;
				logger.error("BigQuery error", e);
				throw new BigQueryItemWriterException("Error on write happened", e);
			}
			finally {
				if (StringUtils.hasText(streamName)) {
					final long rowCount = bigQueryWriteClient.finalizeWriteStream(streamName).getRowCount();
					if (chunk.size() != rowCount) {
						logger.warn("Finalized response row count=%d is not the same as chunk size=%d"
							.formatted(rowCount, chunk.size()));
					}
				}

				if (!writeFailed && logger.isDebugEnabled()) {
					logger.debug("Write operation submitted: " + bigQueryWriteCounter.incrementAndGet());
				}
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.bigQueryWriteClient, "BigQuery write client must be provided");
		Assert.notNull(this.tableName, "Table name must be provided");
		Assert.notNull(this.marshaller, "Marshaller must be provided");

		if (this.apiFutureCallback != null) {
			Assert.notNull(this.executor, "Executor must be provided");
		}
	}

	/**
	 * GRPC client that wraps communication with BigQuery.
	 * @param bigQueryWriteClient a client
	 */
	public void setBigQueryWriteClient(final BigQueryWriteClient bigQueryWriteClient) {
		this.bigQueryWriteClient = bigQueryWriteClient;
	}

	/**
	 * A full path to the BigQuery table.
	 * @param tableName a name
	 */
	public void setTableName(final TableName tableName) {
		this.tableName = tableName;
	}

	/**
	 * Converter that transforms a single row into a {@link String}.
	 * @param marshaller your JSON mapper
	 */
	public void setMarshaller(final JsonObjectMarshaller<T> marshaller) {
		this.marshaller = marshaller;
	}

	/**
	 * {@link ApiFutureCallback} that will be called in case of successful of failed
	 * response.
	 * @param apiFutureCallback a callback
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setExecutor(Executor)
	 */
	public void setApiFutureCallback(final ApiFutureCallback<AppendRowsResponse> apiFutureCallback) {
		this.apiFutureCallback = apiFutureCallback;
	}

	/**
	 * An {@link Executor} that will be calling a {@link ApiFutureCallback}.
	 * @param executor an executor
	 * @see BigQueryWriteApiCommitedJsonItemWriter#setApiFutureCallback(ApiFutureCallback)
	 */
	public void setExecutor(final Executor executor) {
		this.executor = executor;
	}

}
