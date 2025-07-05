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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class S3MultipartUploaderTests {

	private S3Client s3Client;

	private S3MultipartUploader s3MultipartUploader;

	@BeforeEach
	void setUp() {
		this.s3Client = mock(S3Client.class);
		var s3Uploader = new S3MultipartUploader(this.s3Client, "bucket", "key");
		s3Uploader.setPartSize(5);
		this.s3MultipartUploader = s3Uploader;
	}

	@Test
	void testUpload_SuccessfulUpload() throws IOException {
		byte[] data = "HelloWorld!".getBytes(); // 11 bytes, 3 parts, 2 of 5 bytes each
												// and one of 1 byte
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

		// given
		given(this.s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
			.willReturn(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());

		given(this.s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
			.willReturn(UploadPartResponse.builder().eTag("etag1").build(),
						UploadPartResponse.builder().eTag("etag2").build(),
						UploadPartResponse.builder().eTag("etag3").build());

		given(this.s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
			.willReturn(CompleteMultipartUploadResponse.builder().location("url").build());

		// when
		this.s3MultipartUploader.upload(inputStream);

		// then
		then(this.s3Client).should().createMultipartUpload(any(CreateMultipartUploadRequest.class));
		then(this.s3Client).should(times(3)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
		then(this.s3Client).should().completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
		then(this.s3Client).should().close();
	}

	@Test
	void testUpload_AbortOnException() throws IOException {
		byte[] data = "HelloWorld".getBytes();
		ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

		// given
		given(this.s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
			.willReturn(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());

		given(this.s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
			.willThrow(new RuntimeException("Upload failed"));

		// when/then
		assertThatThrownBy(() -> this.s3MultipartUploader.upload(inputStream))
			.isInstanceOf(RuntimeException.class);
		then(this.s3Client).should().abortMultipartUpload(any(AbortMultipartUploadRequest.class));
		then(this.s3Client).should().close();
	}

}
