/*
 * Copyright 2006-2022 the original author or authors.
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

import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.batch.extensions.s3.serializer.S3Serializer;
import org.springframework.batch.extensions.s3.stream.S3MultipartOutputStream;
import org.springframework.batch.extensions.s3.stream.S3MultipartUploader;
import org.springframework.batch.extensions.s3.stream.S3OutputStream;
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

	public static class Builder<T> {
		private S3Client s3Client;

		private String bucket;

		private String key;

		private S3Serializer<T> serializer;

		private boolean multipartUpload = false;

		private String contentType;

		private Integer partSize;

		public Builder<T>  s3Client(S3Client s3Client) {
			this.s3Client = s3Client;
			return this;
		}

		public Builder<T> bucketName(String bucketName) {
			this.bucket = bucketName;
			return this;
		}

		public Builder<T> objectKey(String key) {
			this.key = key;
			return this;
		}

		public Builder<T>  serializer(S3Serializer<T> serializer) {
			this.serializer = serializer;
			return this;
		}

		public Builder<T> multipartUpload(boolean multipartUpload) {
			this.multipartUpload = multipartUpload;
			return this;
		}

		public Builder<T>  partSize(int partSize) {
			this.partSize = partSize;
			return this;
		}

		public Builder<T>  contentType(String contentType) {
			this.contentType = contentType;
			return this;
		}

		public  S3ItemWriter<T> build() throws IOException {
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

			return new S3ItemWriter<T>(outputStream,  this.serializer);
		}
	}

}
