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

import java.io.IOException;
import java.io.OutputStream;

import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.batch.extensions.s3.S3ItemWriter;
import org.springframework.batch.extensions.s3.serializer.S3Serializer;
import org.springframework.batch.extensions.s3.stream.S3MultipartOutputStream;
import org.springframework.batch.extensions.s3.stream.S3MultipartUploader;
import org.springframework.batch.extensions.s3.stream.S3OutputStream;

public class S3ItemWriterBuilder<T> {
	@Nullable
	private S3Client s3Client;

	@Nullable
	private String bucket;

	@Nullable
	private String key;

	@Nullable
	private S3Serializer<T> serializer;

	private boolean multipartUpload;

	@Nullable
	private String contentType;

	@Nullable
	private Integer partSize;

	public S3ItemWriterBuilder<T>  s3Client(S3Client s3Client) {
		this.s3Client = s3Client;
		return this;
	}

	public S3ItemWriterBuilder<T> bucketName(String bucketName) {
		this.bucket = bucketName;
		return this;
	}

	public S3ItemWriterBuilder<T> objectKey(String key) {
		this.key = key;
		return this;
	}

	public S3ItemWriterBuilder<T>  serializer(S3Serializer<T> serializer) {
		this.serializer = serializer;
		return this;
	}

	public S3ItemWriterBuilder<T> multipartUpload(boolean multipartUpload) {
		this.multipartUpload = multipartUpload;
		return this;
	}

	public S3ItemWriterBuilder<T>  partSize(int partSize) {
		this.partSize = partSize;
		return this;
	}

	public S3ItemWriterBuilder<T>  contentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	public S3ItemWriter<T> build() throws IOException {
		if (this.s3Client == null || this.bucket == null || this.key == null || this.serializer == null) {
			throw new IllegalArgumentException("S3Client, bucket, key, and serializer must be provided");
		}
		OutputStream outputStream;
		if (this.multipartUpload) {
			S3MultipartUploader s3MultipartUploader = new S3MultipartUploader(this.s3Client, this.bucket, this.key);
			if (this.contentType != null) {
				s3MultipartUploader.setContentType(this.contentType);
			}
			if (this.partSize != null) {
				s3MultipartUploader.setPartSize(this.partSize);
			}

			outputStream = new S3MultipartOutputStream(s3MultipartUploader);
		}
		else {
			outputStream = new S3OutputStream(this.s3Client, this.bucket, this.key);
			if (this.contentType != null) {
				((S3OutputStream) outputStream).setContentType(this.contentType);
			}
		}

		return new S3ItemWriter<>(outputStream,  this.serializer);
	}
}
