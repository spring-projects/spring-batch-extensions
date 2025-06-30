package org.springframework.batch.extensions.bigquery.emulator.writer.writeapi.json;

import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.TableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.NameUtils;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.ResultVerifier;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.writer.base.EmulatorBaseItemWriterTest;
import org.springframework.batch.extensions.bigquery.writer.writeapi.json.BigQueryWriteApiCommitedJsonItemWriter;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

class EmulatorBigQueryWriteApiCommitedJsonItemWriterTest extends EmulatorBaseItemWriterTest {

    @Test
    void testWrite() throws Exception {
        AtomicBoolean consumerCalled = new AtomicBoolean();
        TableId tableId = TableId.of(TestConstants.PROJECT, TestConstants.DATASET, NameUtils.generateTableName(TestConstants.JSON));
        TableDefinition tableDefinition = StandardTableDefinition.of(PersonDto.getBigQuerySchema());
        bigQuery.create(TableInfo.of(tableId, tableDefinition));

        Chunk<PersonDto> expected = TestConstants.CHUNK;

        BigQueryWriteApiCommitedJsonItemWriter<Object> writer = new BigQueryWriteApiCommitedJsonItemWriter<>();
        writer.setBigQueryWriteClient(bigQueryWriteClient);
        writer.setTableName(TableName.of(tableId.getProject(), tableId.getDataset(), tableId.getTable()));
        writer.setMarshaller(new JacksonJsonObjectMarshaller<>());
        writer.setApiFutureCallback(new ApiFutureCallback<>() {
            @Override
            public void onFailure(Throwable t) {}

            @Override
            public void onSuccess(AppendRowsResponse result) {
                consumerCalled.set(true);
            }
        });
        writer.setExecutor(Executors.newSingleThreadExecutor());

        writer.write(expected);

        ResultVerifier.verifyTableResult(expected, bigQuery.listTableData(tableId));
        Assertions.assertTrue(consumerCalled.get());
    }

}
