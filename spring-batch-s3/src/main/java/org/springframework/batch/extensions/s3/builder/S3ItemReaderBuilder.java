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

package org.springframework.batch.extensions.s3.builder;

import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.batch.extensions.s3.S3ItemReader;
import org.springframework.batch.extensions.s3.serializer.S3Deserializer;
import org.springframework.batch.extensions.s3.stream.S3InputStream;

public class S3ItemReaderBuilder<T> {
	private S3Client s3Client;

	private String bucketName;

	private String objectKey;

	private S3Deserializer<T> deserializer;

	private Integer bufferSize;

	public S3ItemReaderBuilder<T> s3Client(S3Client s3Client) {
		this.s3Client = s3Client;
		return this;
	}

	public S3ItemReaderBuilder<T> bucketName(String bucketName) {
		this.bucketName = bucketName;
		return this;
	}

	public S3ItemReaderBuilder<T> objectKey(String objectKey) {
		this.objectKey = objectKey;
		return this;
	}

	public S3ItemReaderBuilder<T> deserializer(S3Deserializer<T> deserializer) {
		this.deserializer = deserializer;
		return this;
	}

	public S3ItemReaderBuilder<T> bufferSize(int bufferSize) {
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
