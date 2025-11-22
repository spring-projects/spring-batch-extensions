package org.springframework.batch.extensions.bigquery.unit.writer.loadjob.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.generated.PersonAvroDto;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.writer.loadjob.parquet.BigQueryLoadJobParquetItemWriter;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class BigQueryLoadJobParquetItemWriterTest {

	@Test
	void testSetSchema() throws IllegalAccessException, NoSuchFieldException {
		BigQueryLoadJobParquetItemWriter reader = new BigQueryLoadJobParquetItemWriter();
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryLoadJobParquetItemWriter.class,
				MethodHandles.lookup());
		Schema expected = PersonDto.getAvroSchema();

		reader.setSchema(expected);

		Schema actual = (Schema) handle.findVarHandle(BigQueryLoadJobParquetItemWriter.class, "schema", Schema.class)
			.get(reader);

		Assertions.assertEquals(expected, actual);

	}

	@Test
	void testSetCodecName() throws IllegalAccessException, NoSuchFieldException {
		BigQueryLoadJobParquetItemWriter reader = new BigQueryLoadJobParquetItemWriter();
		MethodHandles.Lookup handle = MethodHandles.privateLookupIn(BigQueryLoadJobParquetItemWriter.class,
				MethodHandles.lookup());
		CompressionCodecName expected = CompressionCodecName.GZIP;

		reader.setCodecName(expected);

		CompressionCodecName actual = (CompressionCodecName) handle
			.findVarHandle(BigQueryLoadJobParquetItemWriter.class, "codecName", CompressionCodecName.class)
			.get(reader);

		Assertions.assertEquals(expected, actual);

	}

	@Test
	void testConvertObjectsToByteArrays_GenericRecord() {
		TestWriter writer = new TestWriter();
		writer.setSchema(PersonDto.getAvroSchema());
		writer.setCodecName(CompressionCodecName.UNCOMPRESSED);

		// Empty
		Assertions.assertTrue(writer.testConvertObjectsToByteArrays(List.of()).isEmpty());

		// Not empty
		List<byte[]> actual = writer.testConvertObjectsToByteArrays(TestConstants.AVRO_GENERIC_CHUNK.getItems());
		List<byte[]> expected = convert(TestConstants.AVRO_GENERIC_CHUNK.getItems());

		for (int i = 0; i < expected.size(); i++) {
			Assertions.assertArrayEquals(expected.get(i), actual.get(i));
		}
	}

	@Test
	void testConvertObjectsToByteArrays_GeneratedRecord() {
		TestWriter writer = new TestWriter();
		writer.setSchema(PersonAvroDto.getClassSchema());
		writer.setCodecName(CompressionCodecName.UNCOMPRESSED);

		// Empty
		Assertions.assertTrue(writer.testConvertObjectsToByteArrays(List.of()).isEmpty());

		// Not empty
		List<byte[]> actual = writer.testConvertObjectsToByteArrays(TestConstants.AVRO_GENERATED_CHUNK.getItems());
		List<byte[]> expected = convert(TestConstants.AVRO_GENERATED_CHUNK.getItems());

		for (int i = 0; i < expected.size(); i++) {
			Assertions.assertArrayEquals(expected.get(i), actual.get(i));
		}
	}

	@Test
	void testPerformFormatSpecificChecks() {
		TestWriter writer = new TestWriter();

		// Schema
		IllegalArgumentException actual = Assertions.assertThrows(IllegalArgumentException.class,
				writer::testPerformFormatSpecificChecks);
		Assertions.assertEquals("Schema must be provided", actual.getMessage());

		// Codec
		writer.setSchema(PersonDto.getAvroSchema());
		actual = Assertions.assertThrows(IllegalArgumentException.class, writer::testPerformFormatSpecificChecks);
		Assertions.assertEquals("Codec must be provided", actual.getMessage());
	}

	private List<byte[]> convert(List<? extends GenericRecord> items) {
		Path tempFile = null;
		try {
			tempFile = Files.createTempFile("test-", null);

			final ParquetWriter<GenericRecord> writer = AvroParquetWriter
				.<GenericRecord>builder(new LocalOutputFile(tempFile))
				.withSchema(items.get(0).getSchema())
				.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
				.withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
				.build();

			try (writer) {
				for (final GenericRecord item : items) {
					writer.write(item);
				}
			}
			return List.of(Files.readAllBytes(tempFile));
		}
		catch (IOException e) {
			return List.of();
		}
		finally {
			try {
				Files.deleteIfExists(tempFile);
			}
			catch (IOException e) {
				// Ignored
			}
		}
	}

	private static final class TestWriter extends BigQueryLoadJobParquetItemWriter {

		void testPerformFormatSpecificChecks() {
			performFormatSpecificChecks();
		}

		List<byte[]> testConvertObjectsToByteArrays(List<? extends GenericRecord> list) {
			return convertObjectsToByteArrays(list);
		}

	}

}
