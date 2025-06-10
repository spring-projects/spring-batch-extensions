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

package org.springframework.batch.extensions.bigquery.emulator.base;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import org.testcontainers.containers.GenericContainer;

public abstract class BaseEmulatorTest {

    protected static final String PROJECT = "batch-test";

    protected static BigQueryOptions.Builder prepareBigQueryBuilder() {
        return BigQueryOptions
                .newBuilder()
                .setProjectId(PROJECT)
                .setCredentials(NoCredentials.getInstance());
    }

    protected static String getBigQueryUrl(GenericContainer<?> container) {
        return "http://%s:%d".formatted(container.getHost(), container.getMappedPort(BigQueryBaseDockerConfiguration.PORT));
    }
}
