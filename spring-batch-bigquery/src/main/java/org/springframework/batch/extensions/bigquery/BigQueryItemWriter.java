/*
 * Copyright 2002-2021 the original author or authors.
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

package org.springframework.batch.extensions.bigquery;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
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
import com.google.common.collect.Iterables;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link ItemWriter} for Google BigQuery.
 * This writer uses java client from Google, so we cannot control this flow fully.
 * Take into account that this writer produces {@link com.google.cloud.bigquery.JobConfiguration.Type#LOAD} {@link Job}.
 *
 * <p>Supported formats:
 * <ul>
 *     <li>JSON</li>
 *     <li>CSV</li>
 * </ul>
 *
 * <p>For example if you generate {@link TableDataWriteChannel} and you {@link TableDataWriteChannel#close()} it,
 * there is no guarantee that single {@link com.google.cloud.bigquery.Job} will be created.
 *
 * <p>It does not support save state feature. It is thread-safe.
 * Take into account that BigQuery has rate limits and it is very easy to exceed those in concurrent environment.
 * @see <a href="https://cloud.google.com/bigquery/quotas">BigQuery Quotas & Limits</a>
 *
 * @author Vova Perebykivskyi
 * @since 0.1.0
 * @see <a href="https://cloud.google.com/bigquery">BigQuery</a>
 * @see <a href="https://github.com/googleapis/java-bigquery">BigQuery Java Client on GitHub</a>
 */
public class BigQueryItemWriter<T> implements ItemWriter<T>, InitializingBean {

    private final Log logger = LogFactory.getLog(getClass());
    private final AtomicLong bigQueryWriteCounter = new AtomicLong();

    /**
     * Used for simple conversion.
     */
    private Converter<T, byte[]> rowMapper;
    private ObjectWriter objectWriter;
    private Class itemClass;

    private BigQuery bigQuery;

    /**
     * You can specify here some specific dataset configuration, like location.
     * This dataset will be created.
     */
    private DatasetInfo datasetInfo;

    private WriteChannelConfiguration writeChannelConfig;

    /**
     * Your custom logic with {@link Job}.
     * {@link Job} will be assigned after {@link TableDataWriteChannel#close()}.
     */
    private Consumer<Job> jobConsumer;

    private Table getTable() {
        return this.bigQuery.getTable(this.writeChannelConfig.getDestinationTable());
    }

    public void setRowMapper(Converter<T, byte[]> rowMapper) {
        this.rowMapper = rowMapper;
    }

    public void setDatasetInfo(DatasetInfo datasetInfo) {
        this.datasetInfo = datasetInfo;
    }

    public void setJobConsumer(Consumer<Job> consumer) {
        this.jobConsumer = consumer;
    }

    public void setWriteChannelConfig(WriteChannelConfiguration writeChannelConfig) {
        this.writeChannelConfig = writeChannelConfig;
    }

    public void setBigQuery(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        if (CollectionUtils.isNotEmpty(items)) {
            initializeProperties(items);

            if (this.logger.isDebugEnabled()) {
                this.logger.debug(String.format("Mapping %d elements", items.size()));
            }

            ByteBuffer byteBuffer = mapDataToBigQueryFormat(items);
            doWriteDataToBigQuery(byteBuffer);
        }
    }

    /** Actual type of incoming data can be obtained only in runtime */
    private synchronized void initializeProperties(List<? extends T> items) {
        if (Objects.isNull(this.itemClass)) {
            if (isAvro() || isCsv() || isJson()) {
                T firstItem = items.stream().findFirst().orElseThrow(RuntimeException::new);
                this.itemClass = firstItem.getClass();

                if (isAvro()) {
                    boolean isAvroClass = SpecificRecordBase.class.isAssignableFrom(this.itemClass);
                    Assert.isTrue(isAvroClass, "Avro class expected");
                }

                if (Objects.isNull(this.rowMapper)) {
                    if (isCsv()) {
                        this.objectWriter = new CsvMapper().writerWithTypedSchemaFor(this.itemClass);
                    }
                    else if (isJson()) {
                        this.objectWriter = new ObjectMapper().writerFor(this.itemClass);
                    }
                }

                logger.debug("Writer setup is completed");
            }
        }
    }

    private ByteBuffer mapDataToBigQueryFormat(List<? extends T> items)
            throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

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

    private List<byte[]> convertObjectsToByteArrays(List<? extends T> items)
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        Stream<byte[]> byteArrayStream = Stream.empty();

        if (isJson()) {
            byteArrayStream = getJsonByteArrayStream(items);
        }
        else if (isCsv()) {
            byteArrayStream = getCsvByteArrayStream(items);
        }
        else if (isParquet() || isOrc() || isAvro()) {
            throw new UnsupportedOperationException("Not supported right now");
            /*byteArrayStream = getHadoopPathByteArrayStream(items);*/
            /*byteArrayStream = getAvroByteArrayStream(items);*/
        }

        return byteArrayStream.collect(Collectors.toList());
    }

    /**
     * Row could be read as typical {@link String}.
     */
    private Stream<byte[]> getJsonByteArrayStream(List<? extends T> items) {
        return items
                .stream()
                .map(this::mapItemToCsvOrJson)
                .filter(ArrayUtils::isNotEmpty)
                .map(String::new)
                .map(this::convertToNdJson)
                .filter(value -> !ObjectUtils.isEmpty(value))
                .map(row -> row.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] mapItemToCsvOrJson(T t) {
        byte[] result = null;
        try {
            result = Objects.isNull(rowMapper) ? objectWriter.writeValueAsBytes(t) : rowMapper.convert(t);
        }
        catch (JsonProcessingException e) {
            logger.error("Error during processing of the line: ", e);
        }
        return result;
    }

    /**
     * BigQuery uses ndjson https://github.com/ndjson/ndjson-spec.
     * It is expected that to pass here JSON line generated by
     * {@link com.fasterxml.jackson.databind.ObjectMapper} or any other JSON parser.
     */
    private String convertToNdJson(String json) {
        return json.concat(org.apache.commons.lang3.StringUtils.LF);
    }

    /**
     * Row could be read as typical {@link String}.
     */
    private Stream<byte[]> getCsvByteArrayStream(List<? extends T> items) {
        return items
                .stream()
                .map(this::mapItemToCsvOrJson)
                .filter(ArrayUtils::isNotEmpty)
                .map(String::new)
                .filter(value -> !ObjectUtils.isEmpty(value))
                .map(row -> row.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Generates Avro file and writes it to {@link OutputStream}.
     * @see <a href="https://github.com/GoogleCloudPlatform/bigquery-ingest-avro-dataflow-sample">Avro example</a>
     */
    private Stream<byte[]> getAvroByteArrayStream(List<? extends T> items)
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        SpecificRecordBase objectInstance = (SpecificRecordBase) this.itemClass.getDeclaredConstructor().newInstance();
        SpecificDatumWriter<? super T> avroWriter = new SpecificDatumWriter<>(this.itemClass);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             DataFileWriter<? super T> avroFileWriter = new DataFileWriter<>(avroWriter)) {

            /*
             * Input data - 500 rows.
             * Statistic gathered from BigQuery (com.google.cloud.bigquery.JobStatistics.LoadStatistics#getInputBytes).
             *
             * 41,229      input bytes Avro (no codec)
             * 14,691      input bytes Avro (Deflate lvl 8)
             * 41,122      input bytes CSV
             */
            CodecFactory compressionCodec = CodecFactory.deflateCodec(8);

            /* Order of lines (code) should not be changed */
            avroFileWriter.setCodec(compressionCodec);
            avroFileWriter.create(objectInstance.getSchema(), outputStream);

            for (T item : items) {
                avroFileWriter.append(item);
            }

            /* At this point of time only schema present in output stream */
            avroFileWriter.flush();

            return Stream.of(outputStream.toByteArray());
        }
    }

    /**
     * @return {@link TableDataWriteChannel} that should be closed manually.
     * @see <a href="https://github.com/googleapis/google-cloud-java/blob/969bbeef18f004fd51fd46c5def1ae5c644cae3c/google-cloud-examples/src/main/java/com/google/cloud/examples/bigquery/snippets/BigQuerySnippets.java">Examples</a>
     */
    private TableDataWriteChannel getWriteChannel() {
        return this.bigQuery.writer(this.writeChannelConfig);
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(this.bigQuery, "BigQuery service must be provided");
        Assert.notNull(this.writeChannelConfig, "Write channel configuration must be provided");

        Assert.isTrue(BooleanUtils.isFalse(isBigtable()), "Google BigTable is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isGoogleSheets()), "Google Sheets is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isDatastore()), "Google Datastore is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isParquet()), "Parquet is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isOrc()), "Orc is not supported");
        Assert.isTrue(BooleanUtils.isFalse(isAvro()), "Avro is not supported");

        if (BooleanUtils.isFalse(isAvro())) {
            Table table = getTable();

            if (BooleanUtils.toBoolean(this.writeChannelConfig.getAutodetect())) {
                if ((isCsv() || isJson()) && tableHasDefinedSchema(table) && this.logger.isWarnEnabled()) {
                    this.logger.warn("Mixing autodetect mode with already defined schema may lead to errors on BigQuery side");
                }
            }
            else {
                Assert.notNull(this.writeChannelConfig.getSchema(), "Schema must be provided");
                if (tableHasDefinedSchema(table)) {
                    Assert.isTrue(
                            table.getDefinition().getSchema().equals(this.writeChannelConfig.getSchema()),
                            "Schema should be the same"
                    );
                }
            }
        }
        else {
            Assert.isNull(this.writeChannelConfig.getSchema(), "Avro does not require schema");
            Assert.isNull(this.writeChannelConfig.getAutodetect(), "Avro does not require autodetection");
        }

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

    private boolean isCsv() {
        return FormatOptions.csv().getType().equals(this.writeChannelConfig.getFormat());
    }

    private boolean isJson() {
        return FormatOptions.json().getType().equals(this.writeChannelConfig.getFormat());
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

    private boolean tableHasDefinedSchema(Table table) {
        return Optional
                .ofNullable(table)
                .map(Table::getDefinition)
                .map(TableDefinition.class::cast)
                .map(TableDefinition::getSchema)
                .isPresent();
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

    /**
     * It is expected that for {@link BigQueryItemWriter#isParquet()} and {@link BigQueryItemWriter#isOrc()} you use
     * {@link Path} because there is no convenient way to write records to {@link OutputStream}.
     * Not supported right now.
     */
    private Stream<byte[]> getHadoopPathByteArrayStream(List<? extends T> items) throws IOException {
        Stream<byte[]> result = Stream.empty();

        T firstElement = Iterables.getFirst(items, null);
        Assert.notNull(firstElement, "Collection is empty");
        Class<?> classType = firstElement.getClass();

        if (classType.isAssignableFrom(Path.class)) {
            List<URI> uris = items.stream()
                    .map(Path.class::cast)
                    .map(Path::toUri)
                    .collect(Collectors.toList());

            return getFileBasedByteArrayStream(uris);
        }

        return result;
    }

    private Stream<byte[]> getFileBasedByteArrayStream(List<URI> items) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            for (URI uri : items) {
                try (InputStream inputStream = new FileSystemResource(Paths.get(uri)).getInputStream()) {
                    IOUtils.copy(inputStream, outputStream);
                }
            }
            return Stream.of(outputStream.toByteArray());
        }
    }

}
