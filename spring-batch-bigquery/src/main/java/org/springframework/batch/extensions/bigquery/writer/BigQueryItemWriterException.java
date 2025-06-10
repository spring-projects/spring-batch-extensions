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

package org.springframework.batch.extensions.bigquery.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ItemWriterException;

/**
 * Unchecked {@link Exception} indicating that an error has occurred on during {@link ItemWriter#write(Chunk)}.
 */
public class BigQueryItemWriterException extends ItemWriterException {

    /**
     * Create a new {@link BigQueryItemWriterException} based on a message and another {@link Exception}.
     * @param message the message for this {@link Exception}
     * @param cause the other {@link Exception}
     */
    public BigQueryItemWriterException(String message, Throwable cause) {
        super(message, cause);
    }
}