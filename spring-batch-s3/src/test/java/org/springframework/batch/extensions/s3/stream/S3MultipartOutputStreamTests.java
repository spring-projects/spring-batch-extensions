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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class S3MultipartOutputStreamTests {

	private S3Client s3Client;

	private S3Uploader multipartUploadMock;

	@BeforeEach
	void setUp() throws IOException {
		this.s3Client = mock(S3Client.class);
		this.multipartUploadMock = mock(S3Uploader.class);

		given(this.multipartUploadMock.upload(any())).willAnswer((invocation) -> {
			Thread.sleep(100); // Simulate some delay for upload
			return 1L;
		});
	}

	@Test
	void testWriteSingleByteTriggersUpload() throws IOException {
		int testByte = 42;

		try (S3MultipartOutputStream out = new S3MultipartOutputStream(this.multipartUploadMock)) {
			// when
			out.write(testByte);

			ArgumentCaptor<InputStream> captor = ArgumentCaptor.forClass(InputStream.class);

			// then
			then(this.multipartUploadMock).should().upload(captor.capture());
			assertThat(captor.getValue().available()).as("InputStream should contain one byte").isEqualTo(1);
		}
	}

	@Test
	void testConstructorWithDefaultPartSize() throws IOException {
		S3MultipartOutputStream out = new S3MultipartOutputStream(this.s3Client, "bucket", "key");
		out.close();
	}

	@Test
	void testConstructorWithCustomPartSize() throws IOException {
		int customPartSize = 10 * 1024 * 1024;
		var s3Uploader = new S3MultipartUploader(this.s3Client, "bucket", "key");
		s3Uploader.setPartSize(customPartSize);
		S3MultipartOutputStream out = new S3MultipartOutputStream(s3Uploader);
		out.close();
	}

	@Test
	void testConstructorWithS3UploadOutputStream() throws IOException {
		S3MultipartOutputStream out = new S3MultipartOutputStream(this.multipartUploadMock);
		out.close();
	}

}
