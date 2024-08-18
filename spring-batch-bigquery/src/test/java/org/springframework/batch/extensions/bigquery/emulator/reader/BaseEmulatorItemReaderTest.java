package org.springframework.batch.extensions.bigquery.emulator.reader;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
abstract class BaseEmulatorItemReaderTest {
    private static final int PORT = 9050;

    private static final String PROJECT = "batch-test";

    @Container
    private static final GenericContainer<?> CONTAINER = new GenericContainer<>("ghcr.io/goccy/bigquery-emulator")
            .withExposedPorts(PORT)
            .withCommand("--project=" + PROJECT, "--data-from-yaml=/test-data.yaml")
            .withCopyFileToContainer(MountableFile.forClasspathResource("test-data.yaml"), "/test-data.yaml");

    protected static BigQuery bigQuery;

    @BeforeAll
    static void init() {
        bigQuery = BigQueryOptions
                .newBuilder()
                .setHost("http://%s:%d".formatted(CONTAINER.getHost(), CONTAINER.getMappedPort(PORT)))
                .setProjectId(PROJECT)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }
}