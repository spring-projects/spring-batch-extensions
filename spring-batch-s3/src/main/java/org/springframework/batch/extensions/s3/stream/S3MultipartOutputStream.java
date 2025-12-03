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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * An {@link OutputStream} that writes data to an S3 object using multipart upload. It
 * uses a {@link PipedInputStream} and a {@link PipedOutputStream} to allow writing data
 * asynchronously while uploading it in parts. This stream is suitable for large file
 * uploads.
 *
 * @author Andrea Cioni
 */
public class S3MultipartOutputStream extends OutputStream {

	private static final Logger logger = LoggerFactory.getLogger(S3MultipartOutputStream.class);

	private final PipedInputStream pipedInputStream;

	private final PipedOutputStream pipedOutputStream;

	private  ExecutorService singleThreadExecutor;

	private volatile boolean uploading;

	private final S3Uploader multipartUpload;

	public S3MultipartOutputStream(S3Client s3Client, String bucketName, String key) throws IOException {
		this(new S3MultipartUploader(s3Client, bucketName, key));
	}

	public S3MultipartOutputStream(S3Uploader s3Uploader) throws IOException {
		this.pipedInputStream = new PipedInputStream();
		this.pipedOutputStream = new PipedOutputStream(this.pipedInputStream);
		this.uploading = false;
		this.multipartUpload = s3Uploader;
	}

	@Override
	public void write(int b) throws IOException {
		if (!this.uploading) {
			this.uploading = true;

			startUpload();
		}
		this.pipedOutputStream.write(b);
	}

	private void startUpload() {
		if (this.singleThreadExecutor == null) {
			this.singleThreadExecutor = Executors.newSingleThreadExecutor();
		}

		this.singleThreadExecutor.execute(() -> {
			try {
				this.multipartUpload.upload(this.pipedInputStream);
			}
			catch (IOException ex) {
				logger.error("Error during multipart upload", ex);
				throw new RuntimeException(ex);
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

		if (this.uploading) {
			try {
				if (!this.singleThreadExecutor.awaitTermination(10L, TimeUnit.SECONDS)) {
					logger.warn("Multipart upload thread did not finish in time");
				}
			}
			catch (InterruptedException ex) {
				logger.error("Multipart upload thread interrupted", ex);
			}
		}

		logger.debug("Output stream closed");
		super.close();
	}

	public void setSingleThreadExecutor(ExecutorService singleThreadExecutor) {
		this.singleThreadExecutor = singleThreadExecutor;
	}
}
