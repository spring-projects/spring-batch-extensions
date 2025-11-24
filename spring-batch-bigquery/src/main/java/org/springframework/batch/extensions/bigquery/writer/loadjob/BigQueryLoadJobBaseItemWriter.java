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

package org.springframework.batch.extensions.bigquery.writer.loadjob;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.extensions.bigquery.writer.BigQueryItemWriterException;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Base class that holds shared code for load job JSON and CSV writers.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.1.0
 */
public abstract class BigQueryLoadJobBaseItemWriter<T> implements ItemWriter<T>, InitializingBean {

	/**
	 * Default constructor
	 */
	protected BigQueryLoadJobBaseItemWriter() {
	}

	/** Logger that can be reused */
	protected final Log logger = LogFactory.getLog(getClass());

	private final AtomicLong bigQueryWriteCounter = new AtomicLong();

	/**
	 * Describes what should be written (format) and its destination (table).
	 */
	protected WriteChannelConfiguration writeChannelConfig;

	/**
	 * You can specify here some specific dataset configuration, like location. This
	 * dataset will be created.
	 */
	private DatasetInfo datasetInfo;

	/**
	 * Your custom logic with {@link Job}.
	 * <p>
	 * {@link Job} will be assigned after {@link TableDataWriteChannel#close()}.
	 */
	private Consumer<Job> jobConsumer;

	private BigQuery bigQuery;

	private boolean writeFailed;

	/**
	 * Fetches table from the provided configuration.
	 * @return {@link Table} that is described in
	 * {@link BigQueryLoadJobBaseItemWriter#writeChannelConfig}
	 */
	protected Table getTable() {
		return this.bigQuery.getTable(this.writeChannelConfig.getDestinationTable());
	}

	/**
	 * Provides additional information about the
	 * {@link com.google.cloud.bigquery.Dataset}.
	 * @param datasetInfo BigQuery dataset info
	 */
	public void setDatasetInfo(final DatasetInfo datasetInfo) {
		this.datasetInfo = datasetInfo;
	}

	/**
	 * Callback when {@link Job} will be finished.
	 * @param consumer your consumer
	 */
	public void setJobConsumer(final Consumer<Job> consumer) {
		this.jobConsumer = consumer;
	}

	/**
	 * Describes what should be written (format) and its destination (table).
	 * @param writeChannelConfig BigQuery channel configuration
	 */
	public void setWriteChannelConfig(final WriteChannelConfiguration writeChannelConfig) {
		this.writeChannelConfig = writeChannelConfig;
	}

	/**
	 * BigQuery service, responsible for API calls.
	 * @param bigQuery BigQuery service
	 */
	public void setBigQuery(final BigQuery bigQuery) {
		this.bigQuery = bigQuery;
	}

	@Override
	public void write(final Chunk<? extends T> chunk) throws Exception {
		if (!chunk.isEmpty()) {
			final List<? extends T> items = chunk.getItems();
			doInitializeProperties(items);

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Mapping %d elements", items.size()));
			}

			doWriteDataToBigQuery(mapDataToBigQueryFormat(items));
		}
	}

	private ByteBuffer mapDataToBigQueryFormat(final List<? extends T> items) throws IOException {
		try (final var outputStream = new ByteArrayOutputStream()) {
			final List<byte[]> bytes = convertObjectsToByteArrays(items);

			for (final byte[] byteArray : bytes) {
				outputStream.write(byteArray);
			}

			// It is extremely important to create larger ByteBuffer.
			// If you call TableDataWriteChannel too many times, it leads to BigQuery
			// exceptions.
			return ByteBuffer.wrap(outputStream.toByteArray());
		}
	}

	private void doWriteDataToBigQuery(final ByteBuffer byteBuffer) {
		if (logger.isDebugEnabled()) {
			logger.debug("Writing data to BigQuery");
		}

		TableDataWriteChannel writeChannel = null;

		try (final TableDataWriteChannel writer = getWriteChannel()) {
			/* TableDataWriteChannel is not thread safe */
			writer.write(byteBuffer);
			writeChannel = writer;
		}
		catch (Exception e) {
			writeFailed = true;
			logger.error("BigQuery error", e);
			throw new BigQueryItemWriterException("Error on write happened", e);
		}
		finally {
			if (!writeFailed) {
				String logMessage = "Write operation submitted: " + bigQueryWriteCounter.incrementAndGet();

				if (writeChannel != null) {
					logMessage += " -- Job ID: " + writeChannel.getJob().getJobId().getJob();
					if (this.jobConsumer != null) {
						this.jobConsumer.accept(writeChannel.getJob());
					}
				}

				if (logger.isDebugEnabled()) {
					logger.debug(logMessage);
				}
			}
		}
	}

	/**
	 * @return {@link TableDataWriteChannel} that should be closed manually.
	 * @see <a href=
	 * "https://github.com/googleapis/google-cloud-java/blob/969bbeef18f004fd51fd46c5def1ae5c644cae3c/google-cloud-examples/src/main/java/com/google/cloud/examples/bigquery/snippets/BigQuerySnippets.java">Examples</a>
	 */
	private TableDataWriteChannel getWriteChannel() {
		return this.bigQuery.writer(this.writeChannelConfig);
	}

	/**
	 * Performs common validation for CSV and JSON types.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.notNull(this.bigQuery, "BigQuery service must be provided");
		Assert.notNull(this.writeChannelConfig, "Write channel configuration must be provided");
		Assert.notNull(this.writeChannelConfig.getFormat(), "Data format must be provided");

		Assert.isTrue(!isBigtable(), "Google BigTable is not supported");
		Assert.isTrue(!isGoogleSheets(), "Google Sheets is not supported");
		Assert.isTrue(!isDatastore(), "Google Datastore is not supported");
		Assert.isTrue(!isParquet(), "Parquet is not supported");
		Assert.isTrue(!isOrc(), "Orc is not supported");
		Assert.isTrue(!isAvro(), "Avro is not supported");
		Assert.isTrue(!isIceberg(), "Iceberg is not supported");

		performFormatSpecificChecks();

		final String dataset = this.writeChannelConfig.getDestinationTable().getDataset();
		if (this.datasetInfo == null) {
			this.datasetInfo = DatasetInfo.newBuilder(dataset).build();
		}
		else {
			boolean datasetEquals = Objects.equals(this.datasetInfo.getDatasetId().getDataset(), dataset);
			Assert.isTrue(datasetEquals, "Dataset should be configured properly");
		}

		createDataset();
	}

	private void createDataset() {
		final TableId tableId = this.writeChannelConfig.getDestinationTable();
		final String datasetToCheck = tableId.getDataset();

		if (datasetToCheck != null && this.bigQuery.getDataset(datasetToCheck) == null && this.datasetInfo != null) {
			this.bigQuery.create(this.datasetInfo);
		}
	}

	private boolean isAvro() {
		return FormatOptions.avro().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isParquet() {
		return FormatOptions.parquet().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isOrc() {
		return FormatOptions.orc().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isBigtable() {
		return FormatOptions.bigtable().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isGoogleSheets() {
		return FormatOptions.googleSheets().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isDatastore() {
		return FormatOptions.datastoreBackup().getType().equals(this.writeChannelConfig.getFormat());
	}

	private boolean isIceberg() {
		return FormatOptions.iceberg().getType().equals(this.writeChannelConfig.getFormat());
	}

	/**
	 * Schema can be computed on the BigQuery side during upload, so it is good to know
	 * when schema is supplied by user manually.
	 * @param table BigQuery table
	 * @return {@code true} if BigQuery {@link Table} has schema already described
	 */
	protected boolean tableHasDefinedSchema(final Table table) {
		return Optional.ofNullable(table)
			.map(Table::getDefinition)
			.map(TableDefinition.class::cast)
			.map(TableDefinition::getSchema)
			.isPresent();
	}

	/**
	 * Method that setting up metadata about chunk that is being processed.
	 * <p>
	 * In reality is called once.
	 * @param items current chunk
	 */
	protected void doInitializeProperties(List<? extends T> items) {
	}

	/**
	 * Converts chunk into a byte array. Each data type should be converted with respect
	 * to its specification.
	 * @param items current chunk
	 * @return {@link List<byte[]>} converted list of byte arrays
	 */
	protected abstract List<byte[]> convertObjectsToByteArrays(List<? extends T> items);

	/**
	 * Performs specific checks that are unique to the format.
	 */
	protected abstract void performFormatSpecificChecks();

}