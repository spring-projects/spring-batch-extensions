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

package org.springframework.batch.extensions.s3.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * An {@link OutputStream} that writes data directly to an S3 object with a specified MIME
 * type (default is application/octet-stream). This stream load the data in-memory and
 * uploads it to S3 as it is written. It uses a {@link PipedInputStream} and a
 * {@link PipedOutputStream} to allow writing data asynchronously while uploading it
 * directly to S3. Is it not safe to use this stream with large file uploads, as it does
 * not handle multipart uploads or large data efficiently. For this use case, check out
 * {@link S3MultipartOutputStream}.
 *
 * @author Andrea Cioni
 */
public class S3OutputStream extends OutputStream {

	private static final Logger logger = LoggerFactory.getLogger(S3OutputStream.class);

	private final S3Client s3;

	private final String bucketName;

	private final String key;

	private final PipedInputStream pipedInputStream;

	private final PipedOutputStream pipedOutputStream;

	private final ExecutorService singleThreadExecutor;

	private volatile boolean uploading;

	private String contentType = Defaults.DEFAULT_CONTENT_TYPE;

	public S3OutputStream(S3Client s3, String bucketName, String key) throws IOException {
		this.s3 = s3;
		this.bucketName = bucketName;
		this.key = key;
		this.pipedInputStream = new PipedInputStream();
		this.pipedOutputStream = new PipedOutputStream(this.pipedInputStream);
		this.singleThreadExecutor = Executors.newSingleThreadExecutor();
		this.uploading = false;
	}

	@Override
	public void write(int b) throws IOException {
		if (!this.uploading) {
			this.uploading = true;
			runUploadThread();
		}
		this.pipedOutputStream.write(b);
	}

	private void runUploadThread() {
		this.singleThreadExecutor.execute(() -> {
			try {
				RequestBody body = RequestBody
					.fromContentProvider(ContentStreamProvider.fromInputStream(this.pipedInputStream), this.contentType);
				this.s3.putObject((builder) -> builder.bucket(this.bucketName).key(this.key), body);
			}
			finally {
				try {
					this.pipedInputStream.close();
				}
				catch (IOException ex) {
					logger.error("Error closing piped input stream", ex);
				}
			}
		});
		this.singleThreadExecutor.shutdown();
	}

	@Override
	public void close() throws IOException {
		logger.debug("Closing output stream");
		this.pipedOutputStream.close();
		logger.debug("Output stream closed");
		super.close();
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getContentType() {
		return this.contentType;
	}
}
