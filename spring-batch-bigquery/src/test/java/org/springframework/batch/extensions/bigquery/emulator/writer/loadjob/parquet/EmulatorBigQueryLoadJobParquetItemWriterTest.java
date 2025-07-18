package org.springframework.batch.extensions.bigquery.emulator.writer.loadjob.parquet;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.NameUtils;
import org.springframework.batch.extensions.bigquery.common.generated.PersonAvroDto;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.ResultVerifier;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.writer.base.EmulatorBaseItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.loadjob.parquet.BigQueryLoadJobParquetItemWriter;

class EmulatorBigQueryLoadJobParquetItemWriterTest extends EmulatorBaseItemWriterTest {

	@Test
	void testWrite_GenericRecord() throws Exception {
		TableId tableId = TableId.of(TestConstants.DATASET, NameUtils.generateTableName(TestConstants.PARQUET));

		WriteChannelConfiguration config = WriteChannelConfiguration.newBuilder(tableId)
			.setFormatOptions(FormatOptions.parquet())
			.setSchema(PersonDto.getBigQuerySchema())
			.build();

		BigQueryLoadJobParquetItemWriter writer = new BigQueryLoadJobParquetItemWriter();
		writer.setSchema(PersonDto.getAvroSchema());
		writer.setBigQuery(bigQuery);
		writer.setWriteChannelConfig(config);
		writer.setCodecName(CompressionCodecName.UNCOMPRESSED);

		writer.write(TestConstants.AVRO_GENERIC_CHUNK);

		ResultVerifier.verifyAvroTableResult(TestConstants.AVRO_GENERIC_CHUNK, bigQuery.listTableData(tableId));
	}

	@Test
	void testWrite_GeneratedRecord() throws Exception {
		TableId tableId = TableId.of(TestConstants.DATASET, NameUtils.generateTableName(TestConstants.PARQUET));

		WriteChannelConfiguration config = WriteChannelConfiguration.newBuilder(tableId)
			.setFormatOptions(FormatOptions.parquet())
			.setSchema(PersonDto.getBigQuerySchema())
			.build();

		BigQueryLoadJobParquetItemWriter writer = new BigQueryLoadJobParquetItemWriter();
		writer.setSchema(PersonAvroDto.getClassSchema());
		writer.setBigQuery(bigQuery);
		writer.setWriteChannelConfig(config);
		writer.setCodecName(CompressionCodecName.UNCOMPRESSED);

		writer.write(TestConstants.AVRO_GENERATED_CHUNK);

		ResultVerifier.verifyAvroTableResult(TestConstants.AVRO_GENERATED_CHUNK, bigQuery.listTableData(tableId));
	}

}
