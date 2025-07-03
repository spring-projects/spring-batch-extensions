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

/**
 * {@link com.google.cloud.bigquery.JobConfiguration.Type#LOAD}
 * {@link com.google.cloud.bigquery.Job}
 *
 * <p>
 * Supported formats:
 * <ul>
 * <li>JSON</li>
 * <li>CSV</li>
 * </ul>
 *
 * <p>
 * If you generate {@link com.google.cloud.bigquery.TableDataWriteChannel} and you
 * {@link com.google.cloud.bigquery.TableDataWriteChannel#close()} it, there is no
 * guarantee that single {@link com.google.cloud.bigquery.Job} will be created.
 */
package org.springframework.batch.extensions.bigquery.writer.loadjob;