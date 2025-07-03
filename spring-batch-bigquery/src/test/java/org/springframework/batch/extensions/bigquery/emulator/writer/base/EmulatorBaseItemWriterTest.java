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

package org.springframework.batch.extensions.bigquery.emulator.writer.base;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.base.EmulatorBaseTest;
import org.springframework.batch.extensions.bigquery.emulator.base.EmulatorBigQueryBaseDockerConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.LogManager;

@Testcontainers
public abstract class EmulatorBaseItemWriterTest extends EmulatorBaseTest {

	@Container
	protected static final GenericContainer<?> BIG_QUERY_CONTAINER = EmulatorBigQueryBaseDockerConfiguration.CONTAINER
		.withCommand("--project=" + TestConstants.PROJECT, "--log-level=debug", "--data-from-yaml=/writer-test.yaml",
				"--database=/test-db")
		.withCopyFileToContainer(MountableFile.forClasspathResource("writer-test.yaml"), "/writer-test.yaml");

	protected static BigQuery bigQuery;

	protected static BigQueryWriteClient bigQueryWriteClient;

	private static WireMockServer wireMockServer;

	static {
		try {
			LogManager.getLogManager()
				.readConfiguration(new ClassPathResource("java-util-logging.properties").getInputStream());
		}
		catch (IOException e) {
			throw new IllegalStateException();
		}
	}

	@BeforeAll
	static void setupAll() throws IOException {
		SpyResponseExtension extension = new SpyResponseExtension();

		wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().extensions(extension));
		wireMockServer.start();
		extension.setWireMockPort(wireMockServer.port());

		wireMockServer.stubFor(WireMock.any(WireMock.urlMatching(".*"))
			.willReturn(WireMock.aResponse().proxiedFrom(getBigQueryUrl(BIG_QUERY_CONTAINER))));

		bigQuery = prepareBigQueryBuilder().setHost(wireMockServer.baseUrl()).build().getService();

		InstantiatingGrpcChannelProvider grpc = BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
			.setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
			.build();

		String endpoint = "127.0.0.1:"
				+ BIG_QUERY_CONTAINER.getMappedPort(EmulatorBigQueryBaseDockerConfiguration.GRPC_PORT);

		BigQueryWriteSettings settings = BigQueryWriteSettings.newBuilder()
			.setCredentialsProvider(NoCredentialsProvider.create())
			.setEndpoint(endpoint)
			.setTransportChannelProvider(grpc)
			.build();

		bigQueryWriteClient = BigQueryWriteClient.create(settings);
	}

	@AfterAll
	static void shutdownAll() {
		Optional.ofNullable(wireMockServer).ifPresent(WireMockServer::stop);
	}

}