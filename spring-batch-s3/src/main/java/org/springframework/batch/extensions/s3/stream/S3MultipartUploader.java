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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * A utility class for performing multipart uploads to Amazon S3. It reads data from an
 * input stream and uploads it in parts to a specified S3 bucket and key. <br>
 * Reference: <a href=
 * "https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/best-practices-s3-uploads.html">Uploading
 * streams to Amazon S3 using the AWS SDK for Java 2.x</a>
 *
 * @author Andrea Cioni
 */
public class S3MultipartUploader implements S3Uploader {

	private static final Logger logger = LoggerFactory.getLogger(S3MultipartUploader.class);

	private final S3Client s3Client;

	private final String bucket;

	private final String key;

	private int partSize = Defaults.DEFAULT_PART_SIZE;

	private String contentType = Defaults.DEFAULT_CONTENT_TYPE;

	public S3MultipartUploader(S3Client s3Client, String bucket, String key) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
	}

	/**
	 * Reads from the input stream into the buffer, attempting to fill the buffer
	 * completely or until the end of the stream is reached.
	 * @param inputStream the input stream to read from
	 * @param buffer the buffer to fill
	 * @return the number of bytes read, or -1 if the end of the stream is reached before
	 * any bytes are read
	 * @throws IOException if an I/O error occurs
	 */
	private static int readFullyOrToEnd(InputStream inputStream, byte[] buffer) throws IOException {
		int totalBytesRead = 0;
		int bytesRead;
		while (totalBytesRead < buffer.length) {
			bytesRead = inputStream.read(buffer, totalBytesRead, buffer.length - totalBytesRead);
			if (bytesRead == -1) {
				break;
			}
			totalBytesRead += bytesRead;
		}
		return (totalBytesRead > 0) ? totalBytesRead : -1;
	}

	@Override
	public long upload(InputStream inputStream) throws IOException {
		String uploadId;
		long totalBytesRead = 0;

		try {
			CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
				.bucket(this.bucket)
				.key(this.key)
				.contentType(this.contentType)
				.build();

			CreateMultipartUploadResponse createResponse = this.s3Client
				.createMultipartUpload(createMultipartUploadRequest);
			uploadId = createResponse.uploadId();
			logger.debug("Started multipart upload with ID: {}", uploadId);

			List<CompletedPart> completedParts = new ArrayList<>();
			int partNumber = 1;
			byte[] buffer = new byte[this.partSize];
			int bytesRead;

			try {
				while ((bytesRead = readFullyOrToEnd(inputStream, buffer)) > 0) {
					totalBytesRead += bytesRead;
					UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
						.bucket(this.bucket)
						.key(this.key)
						.uploadId(uploadId)
						.partNumber(partNumber)
						.build();

					RequestBody requestBody;
					if (bytesRead < this.partSize) {
						byte[] lastPartBuffer = new byte[bytesRead];
						System.arraycopy(buffer, 0, lastPartBuffer, 0, bytesRead);
						requestBody = RequestBody.fromBytes(lastPartBuffer);
					}
					else {
						requestBody = RequestBody.fromBytes(buffer);
					}

					UploadPartResponse uploadPartResponse = this.s3Client.uploadPart(uploadPartRequest, requestBody);
					CompletedPart part = CompletedPart.builder()
						.partNumber(partNumber)
						.eTag(uploadPartResponse.eTag())
						.build();
					completedParts.add(part);

					logger.debug("Uploaded part {} with size {} bytes", partNumber, bytesRead);
					partNumber++;
				}

				CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
					.parts(completedParts)
					.build();

				CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
					.bucket(this.bucket)
					.key(this.key)
					.uploadId(uploadId)
					.multipartUpload(completedMultipartUpload)
					.build();

				CompleteMultipartUploadResponse completeResponse = this.s3Client
					.completeMultipartUpload(completeRequest);
				logger.debug("Multipart upload completed. Object URL: {}", completeResponse.location());
			}
			catch (Exception ex) {
				logger.error("Error during multipart upload: {}", ex.getMessage(), ex);
				if (uploadId != null) {
					AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
						.bucket(this.bucket)
						.key(this.key)
						.uploadId(uploadId)
						.build();
					this.s3Client.abortMultipartUpload(abortRequest);
					logger.warn("Multipart upload aborted");
				}
				throw ex;
			}
			finally {
				try {
					inputStream.close();
				}
				catch (IOException ex) {
					logger.error("Error closing input stream: {}", ex.getMessage(), ex);
				}
			}
		}
		finally {
			this.s3Client.close();
		}

		return totalBytesRead;
	}

	public int getPartSize() {
		return this.partSize;
	}

	public void setPartSize(int partSize) {
		this.partSize = partSize;
	}

	public String getContentType() {
		return this.contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
}
