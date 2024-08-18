/*
 * Copyright 2002-2024 the original author or authors.
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
 * Google BigQuery related functionality.
 * <p>
 * These writers use a Java client from Google, so we cannot control this flow fully.
 * Take into account that this writer produces {@link com.google.cloud.bigquery.JobConfiguration.Type#LOAD} {@link com.google.cloud.bigquery.Job}.
 *
 * <p>Supported formats:
 * <ul>
 *     <li>JSON</li>
 *     <li>CSV</li>
 * </ul>
 *
 * <p>For example if you generate {@link com.google.cloud.bigquery.TableDataWriteChannel} and you {@link com.google.cloud.bigquery.TableDataWriteChannel#close()} it,
 * there is no guarantee that single {@link com.google.cloud.bigquery.Job} will be created.
 * <p>
 * Take into account that BigQuery has rate limits, and it is very easy to exceed those in concurrent environment.
 * <p>
 * Also, worth mentioning that you should ensure ordering of the fields in DTO that you are going to send to the BigQuery.
 * In case of CSV/JSON and Jackson consider using {@link com.fasterxml.jackson.annotation.JsonPropertyOrder}.
 *
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 * @see <a href="https://cloud.google.com/bigquery/">Google BigQuery</a>
 * @see <a href="https://github.com/googleapis/java-bigquery">BigQuery Java Client on GitHub</a>
 * @see <a href="https://cloud.google.com/bigquery/quotas">BigQuery Quotas &amp; Limits</a>
 */
@NonNullApi
package org.springframework.batch.extensions.bigquery.writer;

import org.springframework.lang.NonNullApi;