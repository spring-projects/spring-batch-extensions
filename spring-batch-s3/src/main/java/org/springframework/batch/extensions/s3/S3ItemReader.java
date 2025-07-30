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
import java.util.Arrays;

import org.springframework.batch.extensions.s3.serializer.S3Deserializer;
import org.springframework.batch.extensions.s3.stream.S3InputStream;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

/**
 * An {@link ItemReader} that reads items from an S3 object using a specified
 * deserializer. It uses an {@link S3InputStream} to read the data and a
 * {@link S3Deserializer} to convert the byte array into the desired item type.
 *
 * @param <T> the type of items to read
 * @author Andrea Cioni
 */
public class S3ItemReader<T> implements ItemReader<T>, ItemStream {

	private static final int DEFAULT_BUFFER_SIZE_BYTES = 128;

	private final S3InputStream in;

	private final S3Deserializer<T> deserializer;

	private int bufferSize = DEFAULT_BUFFER_SIZE_BYTES;

	public S3ItemReader(S3InputStream in, S3Deserializer<T> deserializer) {
		this.in = in;
		this.deserializer = deserializer;
	}

	@Override
	public T read() throws Exception {
		T item;

		//before reading more bytes from the input stream get all of the items
		//that may be buffered inside the deserializer (deserializer is stateful!)
		while ((item = this.deserializer.deserialize(new byte[]{})) != null) {
			return item;
		}

		int bytesRead;
		byte[] buffer = new byte[this.bufferSize];
		while ((bytesRead = this.in.read(buffer)) != -1) {
			item = this.deserializer.deserialize(Arrays.copyOf(buffer, bytesRead));
			if (item != null) {
				return item;
			}
		}
		return null;
	}

	@Override
	public void close() throws ItemStreamException {
		try {
			this.in.close();
		}
		catch (IOException ex) {
			throw new ItemStreamException(ex);
		}
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getBufferSize() {
		return this.bufferSize;
	}
}
