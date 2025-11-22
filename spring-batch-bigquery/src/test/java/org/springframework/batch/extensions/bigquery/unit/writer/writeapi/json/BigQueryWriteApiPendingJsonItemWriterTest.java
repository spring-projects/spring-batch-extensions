package org.springframework.batch.extensions.bigquery.unit.writer.writeapi.json;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigquery.storage.v1.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.writer.BigQueryItemWriterException;
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.BigQueryWriteApiPendingJsonItemWriter;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.json.GsonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

class BigQueryWriteApiPendingJsonItemWriterTest {

	private static final TableName TABLE_NAME = TableName.of(TestConstants.PROJECT, TestConstants.DATASET,
			TestConstants.JSON);

	@Test
	void testWrite_Empty() throws Exception {
		BigQueryWriteClient writeClient = Mockito.mock(BigQueryWriteClient.class);
		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		writer.setBigQueryWriteClient(writeClient);

		writer.write(Chunk.of());

		Mockito.verifyNoInteractions(writeClient);
	}

	@Test
	void testWrite_Exception() {
		BigQueryItemWriterException ex = Assertions.assertThrows(BigQueryItemWriterException.class,
				() -> new BigQueryWriteApiPendingJsonItemWriter<>().write(TestConstants.JAVA_RECORD_CHUNK));
		Assertions.assertEquals("Error on write happened", ex.getMessage());
	}

	@Test
	void testWrite() throws Exception {
		WriteStreamName streamName = WriteStreamName.of(TABLE_NAME.getProject(), TABLE_NAME.getDataset(),
				TABLE_NAME.getTable(), "test-stream-1");

		WriteStream writeStream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();
		CreateWriteStreamRequest streamRequest = CreateWriteStreamRequest.newBuilder()
			.setParent(TABLE_NAME.toString())
			.setWriteStream(writeStream)
			.build();

		BigQueryWriteClient writeClient = Mockito.mock(BigQueryWriteClient.class);
		WriteStream generatedWriteStream = WriteStream.newBuilder()
			.setName(streamName.toString())
			.setTableSchema(PersonDto.getWriteApiSchema())
			.build();
		Mockito.when(writeClient.createWriteStream(streamRequest)).thenReturn(generatedWriteStream);
		Mockito.when(writeClient.getWriteStream(Mockito.any(GetWriteStreamRequest.class)))
			.thenReturn(generatedWriteStream);
		Mockito.when(writeClient.getSettings())
			.thenReturn(
					BigQueryWriteSettings.newBuilder().setCredentialsProvider(NoCredentialsProvider.create()).build());
		Mockito.when(writeClient.finalizeWriteStream(streamName.toString()))
			.thenReturn(FinalizeWriteStreamResponse.newBuilder().build());

		BatchCommitWriteStreamsResponse batchResponse = Mockito.mock(BatchCommitWriteStreamsResponse.class);
		Mockito.when(batchResponse.hasCommitTime()).thenReturn(true);

		Mockito.when(writeClient.batchCommitWriteStreams(Mockito.any(BatchCommitWriteStreamsRequest.class)))
			.thenReturn(batchResponse);

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		writer.setTableName(TABLE_NAME);
		writer.setBigQueryWriteClient(writeClient);
		writer.setMarshaller(new JacksonJsonObjectMarshaller<>());

		writer.write(TestConstants.JAVA_RECORD_CHUNK);

		Mockito.verify(writeClient).createWriteStream(streamRequest);
		Mockito.verify(writeClient).finalizeWriteStream(streamName.toString());
	}

	@Test
	void testAfterPropertiesSet() {
		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();

		// bigQueryWriteClient
		IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
				writer::afterPropertiesSet);
		Assertions.assertEquals("BigQuery write client must be provided", ex.getMessage());

		// tableName
		writer.setBigQueryWriteClient(Mockito.mock(BigQueryWriteClient.class));
		ex = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
		Assertions.assertEquals("Table name must be provided", ex.getMessage());

		// marshaller
		writer.setTableName(TABLE_NAME);
		ex = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
		Assertions.assertEquals("Marshaller must be provided", ex.getMessage());

		// executor
		writer.setApiFutureCallback(new TestCallback());
		writer.setMarshaller(new GsonJsonObjectMarshaller<>());
		ex = Assertions.assertThrows(IllegalArgumentException.class, writer::afterPropertiesSet);
		Assertions.assertEquals("Executor must be provided", ex.getMessage());

		// All good
		writer.setExecutor(Executors.newSingleThreadExecutor());
		Assertions.assertDoesNotThrow(writer::afterPropertiesSet);
	}

	@Test
	void testSetBigQueryWriteClient() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		BigQueryWriteClient expected = Mockito.mock(BigQueryWriteClient.class);

		writer.setBigQueryWriteClient(expected);

		BigQueryWriteClient actual = (BigQueryWriteClient) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "bigQueryWriteClient",
					BigQueryWriteClient.class)
			.get(writer);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testSetTableName() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();

		writer.setTableName(TABLE_NAME);

		TableName actual = (TableName) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "tableName", TableName.class)
			.get(writer);
		Assertions.assertEquals(TABLE_NAME, actual);
	}

	@Test
	void testSetMarshaller() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		JsonObjectMarshaller<PersonDto> expected = new JacksonJsonObjectMarshaller<>();

		writer.setMarshaller(expected);

		JsonObjectMarshaller<PersonDto> actual = (JsonObjectMarshaller<PersonDto>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "marshaller", JsonObjectMarshaller.class)
			.get(writer);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testSetApiFutureCallback() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		ApiFutureCallback<AppendRowsResponse> expected = new TestCallback();

		writer.setApiFutureCallback(expected);

		ApiFutureCallback<AppendRowsResponse> actual = (ApiFutureCallback<AppendRowsResponse>) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "apiFutureCallback", ApiFutureCallback.class)
			.get(writer);
		Assertions.assertEquals(expected, actual);
	}

	@Test
	void testSetExecutor() throws IllegalAccessException, NoSuchFieldException {
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryWriteApiPendingJsonItemWriter.class,
				MethodHandles.lookup());

		BigQueryWriteApiPendingJsonItemWriter<PersonDto> writer = new BigQueryWriteApiPendingJsonItemWriter<>();
		Executor expected = Executors.newSingleThreadExecutor();

		writer.setExecutor(expected);

		Executor actual = (Executor) handle
			.findVarHandle(BigQueryWriteApiPendingJsonItemWriter.class, "executor", Executor.class)
			.get(writer);
		Assertions.assertEquals(expected, actual);
	}

	private static final class TestCallback implements ApiFutureCallback<AppendRowsResponse> {

		@Override
		public void onFailure(Throwable t) {
		}

		@Override
		public void onSuccess(AppendRowsResponse result) {
		}

	}

}
