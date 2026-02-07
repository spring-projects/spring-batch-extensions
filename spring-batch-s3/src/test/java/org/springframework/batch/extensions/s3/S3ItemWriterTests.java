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

package org.springframework.batch.extensions.s3;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.s3.serializer.S3Serializer;
import org.springframework.batch.extensions.s3.stream.S3MultipartOutputStream;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemStreamException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;

class S3ItemWriterTests {

	private S3Serializer<String> serializer;

	private S3MultipartOutputStream outputStream;

	@BeforeEach
	void setUp() {
		this.serializer = mock(S3Serializer.class);
		this.outputStream = mock(S3MultipartOutputStream.class);
	}

	@Test
	void testWrite_success() throws Exception {
		String item = "test";
		byte[] data = item.getBytes();
		// given
		given(this.serializer.serialize(item)).willReturn(data);

		S3ItemWriter<String> writer = new S3ItemWriter<>(this.outputStream, this.serializer);
		Chunk<String> chunk = Chunk.of(item);

		// when
		writer.write(chunk);

		// then
		then(this.serializer).should().serialize(item);
		then(this.outputStream).should().write(data);
	}

	@Test
	void testWrite_throwsOnNullOrEmpty() {
		String item = "bad";
		// given
		given(this.serializer.serialize(item)).willReturn(null);

		S3ItemWriter<String> writer = new S3ItemWriter<>(this.outputStream, this.serializer);
		Chunk<String> chunk = Chunk.of(item);

		// when/then
		assertThatThrownBy(() -> writer.write(chunk))
			.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	void testClose_success() throws Exception {
		S3ItemWriter<String> writer = new S3ItemWriter<>(this.outputStream, this.serializer);

		// when
		writer.close();

		// then
		then(this.outputStream).should().close();
	}

	@Test
	void testClose_throwsItemStreamException() throws Exception {
		// given
		willThrow(new IOException("close error")).given(this.outputStream).close();
		S3ItemWriter<String> writer = new S3ItemWriter<>(this.outputStream, this.serializer);

		// when/then
		assertThatThrownBy(writer::close)
			.isInstanceOf(ItemStreamException.class);
	}

}
