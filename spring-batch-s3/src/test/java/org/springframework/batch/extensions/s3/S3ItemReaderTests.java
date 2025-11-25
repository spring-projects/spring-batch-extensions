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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.extensions.s3.serializer.S3Deserializer;
import org.springframework.batch.extensions.s3.serializer.S3StringDeserializer;
import org.springframework.batch.extensions.s3.stream.S3InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

class S3ItemReaderTests {

	private S3Deserializer<String> mockDeserializer;

	private S3InputStream s3InputStream;

	private S3StringDeserializer stringDeserializer;

	@BeforeEach
	void setUp() {
		this.stringDeserializer = new S3StringDeserializer();
		this.mockDeserializer = mock(S3Deserializer.class);
		this.s3InputStream = mock(S3InputStream.class);
	}

	@Test
	void testReadReturnsDeserializedItemWithStreamMock() throws Exception {
		byte[] data = "test".getBytes();
		// given
		given(this.s3InputStream.read(any(byte[].class))).willReturn(data.length, -1);
		given(this.mockDeserializer.deserialize(any(byte[].class))).willReturn(null, "item");

		S3ItemReader<String> reader = new S3ItemReader<>(this.s3InputStream, this.mockDeserializer);

		// when
		String result = reader.read();

		// then
		assertThat(result).isEqualTo("item");
		then(this.s3InputStream).should(times(1)).read(any(byte[].class));
		then(this.mockDeserializer).should(times(2)).deserialize(any(byte[].class));
	}

	@Test
	void testReadReturnsDeserializedItem() throws Exception {
		byte[] data = "item\n".getBytes();

		// given
		given(this.mockDeserializer.deserialize(any(byte[].class)))
				.willReturn(null);
		given(this.s3InputStream.read(any(byte[].class))).willAnswer((invocation) -> {
			byte[] buffer = invocation.getArgument(0);
			System.arraycopy(data, 0, buffer, 0, data.length);
			return data.length;
		}).willReturn(-1);

		S3ItemReader<String> reader = new S3ItemReader<>(this.s3InputStream, this.stringDeserializer);

		// when
		String result = reader.read();

		// then
		assertThat(result).isEqualTo("item");
		then(this.s3InputStream).should(times(1)).read(any(byte[].class));
	}

	@Test
	void testReadReturnsNullWhenNoData() throws Exception {
		// given
		given(this.s3InputStream.read(any(byte[].class))).willReturn(-1);

		S3ItemReader<String> reader = new S3ItemReader<>(this.s3InputStream, this.mockDeserializer);

		// when
		String result = reader.read();

		// then
		assertThat(result).isNull();
	}

	@Test
	void testReadReturnsMultipleItems() throws Exception {
		byte[] data1 = "item1\n".getBytes();
		byte[] data2 = "item2\n".getBytes();

		// given
		given(this.s3InputStream.read(any(byte[].class)))
				.willAnswer((invocation) -> {
					byte[] buffer = invocation.getArgument(0);
					System.arraycopy(data1, 0, buffer, 0, data1.length);
					return data1.length;
				});
		given(this.mockDeserializer.deserialize(any(byte[].class)))
				.willReturn("item1")
				.willReturn("item2")
				.willReturn(null); // No more items
		given(this.s3InputStream.read(any(byte[].class)))
				.willAnswer((invocation) -> {
					byte[] buffer = invocation.getArgument(0);
					System.arraycopy(data2, 0, buffer, 0, data2.length);
					return data2.length;
				})
				.willReturn(-1); // End of stream
		S3ItemReader<String> reader = new S3ItemReader<>(this.s3InputStream, this.mockDeserializer);
		String result1 = reader.read();
		String result2 = reader.read();
		String result3 = reader.read();
		// then

		assertThat(result1).isEqualTo("item1");
		assertThat(result2).isEqualTo("item2");
		assertThat(result3).isNull();
		then(this.s3InputStream).should(times(2)).read(any(byte[].class));
		then(this.mockDeserializer).should(times(4)).deserialize(any(byte[].class));
	}

	@Test
	void testReadReturnsMultipleItemsInSingleDeserialization() throws Exception {
		byte[] data = "item1\nitem2\n".getBytes();

		// given
		given(this.s3InputStream.read(any(byte[].class)))
				.willAnswer((invocation) -> {
					byte[] buffer = invocation.getArgument(0);
					System.arraycopy(data, 0, buffer, 0, data.length);
					return data.length;
				}).willAnswer((invocation) -> -1);

		given(this.mockDeserializer.deserialize(any(byte[].class)))
				.willReturn(null) // buffer is empty
				.willReturn("item1")
				.willReturn("item2")
				.willReturn(null);  // End of stream

		S3ItemReader<String> reader = new S3ItemReader<>(this.s3InputStream, this.mockDeserializer);
		String result1 = reader.read();
		String result2 = reader.read();
		String result3 = reader.read();

		// then

		assertThat(result1).isEqualTo("item1");
		assertThat(result2).isEqualTo("item2");
		assertThat(result3).isNull();
		then(this.s3InputStream).should(times(2)).read(any(byte[].class));
		then(this.mockDeserializer).should(times(4)).deserialize(any(byte[].class));
	}

}
