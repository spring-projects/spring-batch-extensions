/*
 * Copyright 2002-2023 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.writer;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class that holds shared code for JSON and CSV writers.
 *
 * @param <T> your DTO type
 * @author Volodymyr Perebykivskyi
 * @since 0.1.0
 */
public abstract class BigQueryBaseItemWriter<T> implements ItemWriter<T> {

    /** Logger that can be reused */
    protected final Log logger = LogFactory.getLog(getClass());
    private final AtomicLong bigQueryWriteCounter = new AtomicLong();

    /**
     * Describes what should be written (format) and its destination (table).
     */
    protected WriteChannelConfiguration writeChannelConfig;

    /**
     * You can specify here some specific dataset configuration, like location.
     * This dataset will be created.
     */
    private DatasetInfo datasetInfo;

    /**
     * Your custom logic with {@link Job}.
     * {@link Job} will be assigned after {@link TableDataWriteChannel#close()}.
     */
    private Consumer<Job> jobConsumer;

    private BigQuery bigQuery;


    /**
     * Fetches table from provided configuration.
     *
     * @return {@link Table} that is described in {@link BigQueryBaseItemWriter#writeChannelConfig}
     */
    protected Table getTable() {
        return this.bigQuery.getTable(this.writeChannelConfig.getDestinationTable());
    }

    /**
     * Provides additional information about the {@link com.google.cloud.bigquery.Dataset}.
     *
     * @param datasetInfo BigQuery dataset info
     */
    public void setDatasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
    }

    /**
     * Callback when {@link Job} will be finished.
     *
     * @param consumer your consumer
     */
    public void setJobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
    }

    /**
     * Describes what should be written (format) and its destination (table).
     *
     * @param writeChannelConfig BigQuery channel configuration
     */
    public void setWriteChannelConfig(WriteChannelConfiguration writeChannelConfig) {
        this.writeChannelConfig = writeChannelConfig;
    }

    /**
     * BigQuery service, responsible for API calls.
     *
     * @param bigQuery BigQuery service
     */
    public void setBigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        if (BooleanUtils.isFalse(chunk.isEmpty())) {
            List<? extends T> items = chunk.getItems();
            doInitializeProperties(items);

            if (this.logger.isDebugEnabled()) {
                this.logger.debug(String.format("Mapping %d elements", items.size()));
            }

            ByteBuffer byteBuffer = mapDataToBigQueryFormat(items);
            doWriteDataToBigQuery(byteBuffer);
        }
    }

    private ByteBuffer mapDataToBigQueryFormat(List<? extends T> items) throws IOException {
        ByteBuffer byteBuffer;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            List<byte[]> data = convertObjectsToByteArrays(items);

            for (byte[] byteArray : data) {
                outputStream.write(byteArray);
            }

            /*
             * It is extremely important to create larger ByteBuffer,
             * if you call TableDataWriteChannel too many times, it leads to BigQuery exceptions.
             */
            byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
        }
        return byteBuffer;
    }

    private void doWriteDataToBigQuery(ByteBuffer byteBuffer) throws IOException {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Writing data to BigQuery");
        }

        TableDataWriteChannel writeChannel = null;

        try (TableDataWriteChannel writer = getWriteChannel()) {
            /* TableDataWriteChannel is not thread safe */
            writer.write(byteBuffer);
            writeChannel = writer;
        }
        finally {
            String logMessage = "Write operation submitted: " + bigQueryWriteCounter.incrementAndGet();

            if (Objects.nonNull(writeChannel)) {
                logMessage += " -- Job ID: " + writeChannel.getJob().getJobId().getJob();
                if (Objects.nonNull(this.jobConsumer)) {
                    this.jobConsumer.accept(writeChannel.getJob());
                }
            }

            if (this.logger.isDebugEnabled()) {
                this.logger.debug(logMessage);
            }
        }
    }

    /**
     * @return {@link TableDataWriteChannel} that should be closed manually.
     * @see <a href="https://github.com/googleapis/google-cloud-java/blob/969bbeef18f004fd51fd46c5def1ae5c644cae3c/google-cloud-examples/src/main/java/com/google/cloud/examples/bigquery/snippets/BigQuerySnippets.java">Examples</a>
     */
    private TableDataWriteChannel getWriteChannel() {
        return this.bigQuery.writer(this.writeChannelConfig);
    }

    /**
     * Performs common validation for CSV and JSON types.
     *
     * @param formatSpecificChecks supplies type-specific validation
     */
    protected void baseAfterPropertiesSet(Supplier<Void> formatSpecificChecks) {
        Assert.notNull(this.bigQuery, "BigQuery service must be provided");
        Assert.notNull(this.writeChannelConfig, "Write channel configuration must be provided");

        Assert.isTrue(BooleanUtils.isFalse(isBigtable()), "Google BigTable is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isGoogleSheets()), "Google Sheets is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isDatastore()), "Google Datastore is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isParquet()), "Parquet is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isOrc()), "Orc is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isAvro()), "Avro is not supported");

        formatSpecificChecks.get();

        Assert.notNull(this.writeChannelConfig.getFormat(), "Data format must be provided");

        String dataset = this.writeChannelConfig.getDestinationTable().getDataset();
        if (Objects.isNull(this.datasetInfo)) {
            this.datasetInfo = DatasetInfo.newBuilder(dataset).build();
        }

        Assert.isTrue(
                Objects.equals(this.datasetInfo.getDatasetId().getDataset(), dataset),
                "Dataset should be configured properly"
        );

        createDataset();
    }

    private void createDataset() {
        TableId tableId = this.writeChannelConfig.getDestinationTable();
        String datasetToCheck = tableId.getDataset();

        if (Objects.nonNull(datasetToCheck)) {
            Dataset foundDataset = this.bigQuery.getDataset(datasetToCheck);

            if (Objects.isNull(foundDataset)) {
                if (Objects.nonNull(this.datasetInfo)) {
                    this.bigQuery.create(this.datasetInfo);
                }
            }
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

    /**
     * Schema can be computed on BigQuery side during upload,
     * so it is good to know when schema is supplied by user manually.
     *
     * @param table BigQuery table
     * @return {@code true} if BigQuery {@link Table} has schema already described
     */
    protected boolean tableHasDefinedSchema(Table table) {
        return Optional
                .ofNullable(table)
                .map(Table::getDefinition)
                .map(TableDefinition.class::cast)
                .map(TableDefinition::getSchema)
                .isPresent();
    }

    /**
     * Method that setting up metadata about chunk that is being processed. In reality is called once.
     *
     * @param items current chunk
     */
    protected abstract void doInitializeProperties(List<? extends T> items);

    /**
     * Converts chunk into byte array.
     * Each data type should be converted with respect to its specification.
     *
     * @param items current chunk
     * @return {@link List<byte[]>} converted list of byte arrays
     */
    protected abstract List<byte[]> convertObjectsToByteArrays(List<? extends T> items);

}
