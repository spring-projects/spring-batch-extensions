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

package org.springframework.batch.extensions.bigquery.emulator.reader.base;

import com.google.cloud.bigquery.BigQuery;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.emulator.base.EmulatorBaseTest;
import org.springframework.batch.extensions.bigquery.emulator.base.EmulatorBigQueryBaseDockerConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public abstract class EmulatorBaseItemReaderTest extends EmulatorBaseTest {

	@Container
	private static final GenericContainer<?> CONTAINER = EmulatorBigQueryBaseDockerConfiguration.CONTAINER
		.withCommand("--project=" + TestConstants.PROJECT, "--log-level=debug", "--data-from-yaml=/reader-test.yaml")
		.withCopyFileToContainer(MountableFile.forClasspathResource("reader-test.yaml"), "/reader-test.yaml");

	protected static BigQuery bigQuery;

	@BeforeAll
	static void init() {
		bigQuery = prepareBigQueryBuilder().setHost(getBigQueryUrl(CONTAINER)).build().getService();
	}

}