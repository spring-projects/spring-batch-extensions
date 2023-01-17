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

package org.springframework.batch.extensions.bigquery.integration.base;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.junit.jupiter.api.TestInfo;

import java.lang.reflect.Method;

public abstract class BaseBigQueryIntegrationTest {

    private static final String TABLE_PATTERN = "%s_%s";

    public final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

    protected String getTableName(TestInfo testInfo) {
        return String.format(
                TABLE_PATTERN,
                testInfo.getTags().stream().findFirst().orElseThrow(),
                testInfo.getTestMethod().map(Method::getName).orElseThrow()
        );
    }
}
