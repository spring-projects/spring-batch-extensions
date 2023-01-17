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

/**
 * In order to launch these tests you should provide a way how to authorize to Google BigQuery.
 * A simple way is to create service account, store credentials as JSON file and provide environment variable.
 * Example: GOOGLE_APPLICATION_CREDENTIALS=/home/dgray/Downloads/bq-key.json
 * <p>
 * Test names should follow this pattern: test1, test2, testN.
 * So later in BigQuery you will see generated table name: csv_test1, csv_test2, csv_testN.
 * This way it will be easier to trace errors in BigQuery.
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#before-you-begin">Authentication</a>
 */
package org.springframework.batch.extensions.bigquery.integration;