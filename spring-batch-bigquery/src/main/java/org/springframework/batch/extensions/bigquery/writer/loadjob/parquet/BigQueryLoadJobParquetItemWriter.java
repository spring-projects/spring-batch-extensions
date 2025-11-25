package org.springframework.batch.extensions.bigquery.writer.loadjob.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.springframework.batch.extensions.bigquery.writer.loadjob.BigQueryLoadJobBaseItemWriter;
import org.springframework.util.Assert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Parquet writer for BigQuery using Load Job.
 *
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://en.wikipedia.org/wiki/Apache_Parquet">Apache Parquet</a>
 */
public class BigQueryLoadJobParquetItemWriter extends BigQueryLoadJobBaseItemWriter<GenericRecord> {

	private Schema schema;

	private CompressionCodecName codecName;

	/**
	 * Default constructor
	 */
	public BigQueryLoadJobParquetItemWriter() {
	}

	/**
	 * A {@link Schema} that is used to identify fields.
	 * @param schema your schema
	 */
	public void setSchema(final Schema schema) {
		this.schema = schema;
	}

	/**
	 * Specifies a codec for a compression algorithm.
	 * @param codecName your codec
	 */
	public void setCodecName(final CompressionCodecName codecName) {
		this.codecName = codecName;
	}

	@Override
	protected List<byte[]> convertObjectsToByteArrays(final List<? extends GenericRecord> items) {
		if (items.isEmpty()) {
			return List.of();
		}

		Path tempFile = null;
		try {
			tempFile = Files.createTempFile("parquet-avro-chunk-", null);

			final ParquetWriter<GenericRecord> writer = AvroParquetWriter
				.<GenericRecord>builder(new LocalOutputFile(tempFile))
				.withSchema(this.schema)
				.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
				.withCompressionCodec(this.codecName)
				.build();

			try (writer) {
				for (final GenericRecord item : items) {
					writer.write(item);
				}
			}
			return List.of(Files.readAllBytes(tempFile));
		}
		catch (IOException e) {
			logger.error(e);
			return List.of();
		}
		finally {
			if (tempFile != null) {
				try {
					Files.deleteIfExists(tempFile);
				}
				catch (IOException e) {
					logger.error(e);
				}
			}
		}
	}

	@Override
	protected void performFormatSpecificChecks() {
		Assert.notNull(this.schema, "Schema must be provided");
		Assert.notNull(this.codecName, "Codec must be provided");
	}

}
