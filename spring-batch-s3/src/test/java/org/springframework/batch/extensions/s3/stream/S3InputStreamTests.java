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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

class S3InputStreamTests {

	private S3Client s3Client;

	private final byte[] data = { 1, 2, 3, 4 };

	@BeforeEach
	void setUp() {
		this.s3Client = Mockito.mock(S3Client.class);
	}

	@Test
	void testRead() throws IOException {
		InputStream mockStream = new ByteArrayInputStream(this.data);
		ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
				GetObjectResponse.builder().build(), mockStream);
		// given
		given(this.s3Client.getObject(any(GetObjectRequest.class))).willReturn(responseInputStream);

		String key = "test-key";
		String bucket = "test-bucket";
		// when
		try (S3InputStream s3InputStream = new S3InputStream(this.s3Client, bucket, key)) {
			for (byte b : this.data) {
				assertThat(s3InputStream.read()).isEqualTo(b);
			}
			assertThat(s3InputStream.read()).isEqualTo(-1);
		}

		// then
		then(this.s3Client).should().getObject(any(GetObjectRequest.class));
	}

}
