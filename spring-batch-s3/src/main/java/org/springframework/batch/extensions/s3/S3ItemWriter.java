/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.batch.extensions.s3;

import java.io.IOException;
import java.io.OutputStream;

import org.springframework.batch.extensions.s3.serializer.S3Serializer;
import org.springframework.batch.extensions.s3.stream.S3MultipartOutputStream;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

/**
 * An {@link ItemWriter} that writes items to an S3 object using a specified serializer.
 * It uses an {@link S3MultipartOutputStream} to write the data and a {@link S3Serializer}
 * to convert the item into a byte array.
 *
 * @param <T> the type of items to write
 * @author Andrea Cioni
 */
public class S3ItemWriter<T> implements ItemWriter<T>, ItemStream {

	private final OutputStream out;

	private final S3Serializer<T> serializer;

	public S3ItemWriter(OutputStream out, S3Serializer<T> serializer) {
		this.out = out;
		this.serializer = serializer;
	}

	@Override
	public void write(Chunk<? extends T> chunk) throws Exception {
		for (T item : chunk.getItems()) {
			byte[] serializedData = this.serializer.serialize(item);
			if (serializedData != null && serializedData.length > 0) {
				this.out.write(serializedData);
			}
			else {
				throw new IllegalArgumentException("Serialized data is null or empty for item: " + item);
			}
		}
	}

	@Override
	public void close() throws ItemStreamException {
		try {
			this.out.close();
		}
		catch (IOException ex) {
			throw new ItemStreamException(ex);
		}
	}
}
