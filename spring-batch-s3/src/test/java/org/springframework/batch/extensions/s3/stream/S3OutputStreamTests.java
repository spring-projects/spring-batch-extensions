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
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class S3OutputStreamTests {

	private S3Client s3Client;

	@BeforeEach
	void setUp() {
		this.s3Client = mock(S3Client.class);
	}

	@Test
	void testWriteAndUpload() throws IOException, InterruptedException {
		byte[] data = { 10, 20, 30, 40 };
		doReturn(null).when(this.s3Client).putObject(any(Consumer.class), any(RequestBody.class));

		String bucket = "test-bucket";
		String key = "test-key";
		try (S3OutputStream out = new S3OutputStream(this.s3Client, bucket, key)) {
			out.write(data);
		}

		verify(this.s3Client, timeout(200)).putObject(any(Consumer.class), any(RequestBody.class));
		verify(this.s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
	}

}
