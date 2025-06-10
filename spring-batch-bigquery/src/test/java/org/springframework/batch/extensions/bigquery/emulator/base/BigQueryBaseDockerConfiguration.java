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

import org.testcontainers.containers.GenericContainer;

public final class BigQueryBaseDockerConfiguration {

    public static final int PORT = 9050;

    public static final GenericContainer<?> CONTAINER = new GenericContainer<>("ghcr.io/goccy/bigquery-emulator:0.6.6")
            .withExposedPorts(PORT);

    private BigQueryBaseDockerConfiguration() {}
}
