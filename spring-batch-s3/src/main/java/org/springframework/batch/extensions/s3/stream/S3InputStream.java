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

package org.springframework.batch.extensions.s3.stream;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * An {@link InputStream} that reads data from an S3 object. It uses the AWS SDK for Java
 * to retrieve the object from S3. Is safe to use this stream for reading large files as
 * it doesn't load the entire file into memory.
 *
 * @author Andrea Cioni
 */
public class S3InputStream extends InputStream {

	private static final Logger logger = LoggerFactory.getLogger(S3InputStream.class);

	private final S3Client s3;

	private final String bucketName;

	private final String objectKey;

	private InputStream inputStream;

	public S3InputStream(S3Client s3, String bucketName, String objectKey) throws IOException {
		this.s3 = s3;
		this.bucketName = bucketName;
		this.objectKey = objectKey;
	}

	@Override
	public int read() throws IOException {
		if (this.inputStream == null) {
			this.inputStream = openS3InputStream();
		}
		return this.inputStream.read();
	}

	@Override
	public void close() throws IOException {
		logger.debug("Closing stream");
		if (this.inputStream != null) {
			this.inputStream.close();
		}
		logger.debug("Stream closed");
		super.close();
	}

	private InputStream openS3InputStream() {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			.bucket(this.bucketName)
			.key(this.objectKey)
			.build();
		return this.s3.getObject(getObjectRequest);
	}

}
