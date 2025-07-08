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

import software.amazon.awssdk.services.s3.S3Client;

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

	private static final int DEFAULT_BUFFER_SIZE = 128;

	private final S3InputStream in;

	private final S3Deserializer<T> deserializer;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

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

	public static class Builder<T> {
		private S3Client s3Client;

		private String bucketName;

		private String objectKey;

		private S3Deserializer<T> deserializer;

		private Integer bufferSize;

		public Builder<T> s3Client(S3Client s3Client) {
			this.s3Client = s3Client;
			return this;
		}

		public Builder<T> bucketName(String bucketName) {
			this.bucketName = bucketName;
			return this;
		}

		public Builder<T> objectKey(String objectKey) {
			this.objectKey = objectKey;
			return this;
		}

		public Builder<T> deserializer(S3Deserializer<T> deserializer) {
			this.deserializer = deserializer;
			return this;
		}

		public Builder<T> bufferSize(int bufferSize) {
			this.bufferSize = bufferSize;
			return this;
		}

		public S3ItemReader<T> build() throws Exception {
			if (this.s3Client == null || this.bucketName == null || this.objectKey == null || this.deserializer == null) {
				throw new IllegalArgumentException("S3Client, bucketName, objectKey, and deserializer must be provided");
			}
			S3InputStream inputStream = new S3InputStream(this.s3Client, this.bucketName, this.objectKey);
			S3ItemReader<T> reader = new S3ItemReader<>(inputStream, this.deserializer);
			if (this.bufferSize != null) {
				reader.setBufferSize(this.bufferSize);
			}
			return reader;
		}
	}
}
