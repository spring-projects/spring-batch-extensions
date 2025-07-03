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

package org.springframework.batch.extensions.bigquery.unit.writer.writeapi.json.builder;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.BigQueryWriteApiPendingJsonItemWriter;
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.builder.BigQueryWriteApiPendingJsonItemWriterBuilder;
import org.springframework.batch.item.json.GsonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

class BigQueryWriteApiPendingJsonItemWriterBuilderTest {

	@Test
	void testBigQueryWriteClient() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriterBuilder.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto> builder = new BigQueryWriteApiPendingJsonItemWriterBuilder<>();
		BigQueryWriteClient expected = Mockito.mock(BigQueryWriteClient.class);

		builder.bigQueryWriteClient(expected);

		BigQueryWriteClient actual = (BigQueryWriteClient) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriterBuilder.class, "bigQueryWriteClient",
					BigQueryWriteClient.class)
			.get(builder);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testTableName() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriterBuilder.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto> builder = new BigQueryWriteApiPendingJsonItemWriterBuilder<>();
		TableName expected = TableName.of(TestConstants.PROJECT, TestConstants.DATASET, TestConstants.JSON);

		builder.tableName(expected);

		TableName actual = (TableName) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriterBuilder.class, "tableName", TableName.class)
			.get(builder);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testMarshaller() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriterBuilder.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto> builder = new BigQueryWriteApiPendingJsonItemWriterBuilder<>();
		JsonObjectMarshaller<PersonDto> expected = new GsonJsonObjectMarshaller<>();

		builder.marshaller(expected);

		JsonObjectMarshaller<PersonDto> actual = (JsonObjectMarshaller<PersonDto>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriterBuilder.class, "marshaller", JsonObjectMarshaller.class)
			.get(builder);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testApiFutureCallback() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriterBuilder.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto> builder = new BigQueryWriteApiPendingJsonItemWriterBuilder<>();

		ApiFutureCallback<AppendRowsResponse> expected = new ApiFutureCallback<>() {
			@Override
			public void onFailure(Throwable t) {
			}

			@Override
			public void onSuccess(AppendRowsResponse result) {
			}
		};

		builder.apiFutureCallback(expected);

		ApiFutureCallback<AppendRowsResponse> actual = (ApiFutureCallback<AppendRowsResponse>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriterBuilder.class, "apiFutureCallback",
					ApiFutureCallback.class)
			.get(builder);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testExecutor() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriterBuilder.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto> builder = new BigQueryWriteApiPendingJsonItemWriterBuilder<>();
		Executor expected = Executors.newSingleThreadExecutor();

		builder.executor(expected);

		Executor actual = (Executor) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriterBuilder.class, "executor", Executor.class)
			.get(builder);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testBuild() throws IOException, IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		JsonObjectMarshaller<PersonDto> expectedMarshaller = new JacksonJsonObjectMarshaller<>();
		BigQueryWriteClient expectedWriteClient = Mockito.mock(BigQueryWriteClient.class);
		Executor expectedExecutor = Executors.newCachedThreadPool();
		TableName expectedTableName = TableName.of(TestConstants.PROJECT, TestConstants.DATASET, TestConstants.JSON);

		ApiFutureCallback<AppendRowsResponse> expectedCallback = new ApiFutureCallback<>() {
			@Override
			public void onFailure(Throwable t) {
			}

			@Override
			public void onSuccess(AppendRowsResponse result) {
			}
		};

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriterBuilder<PersonDto>()
			.marshaller(expectedMarshaller)
			.bigQueryWriteClient(expectedWriteClient)
			.apiFutureCallback(expectedCallback)
			.executor(expectedExecutor)
			.tableName(expectedTableName)
			.build();

		Assertions.assertNotNull(writer);

		JsonObjectMarshaller<PersonDto> actualMarshaller = (JsonObjectMarshaller<PersonDto>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "marshaller", JsonObjectMarshaller.class)
			.get(writer);

		BigQueryWriteClient actualWriteClient = (BigQueryWriteClient) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "bigQueryWriteClient",
					BigQueryWriteClient.class)
			.get(writer);

		ApiFutureCallback<AppendRowsResponse> actualCallback = (ApiFutureCallback<AppendRowsResponse>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "apiFutureCallback", ApiFutureCallback.class)
			.get(writer);

		Executor actualExecutor = (Executor) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "executor", Executor.class)
			.get(writer);

		TableName actualTableName = (TableName) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "tableName", TableName.class)
			.get(writer);

		Assertions.assertEquals(expectedMarshaller, actualMarshaller);
		Assertions.assertEquals(expectedWriteClient, actualWriteClient);
		Assertions.assertEquals(expectedCallback, actualCallback);
		Assertions.assertEquals(expectedExecutor, actualExecutor);
		Assertions.assertEquals(expectedTableName, actualTableName);
	}

}
