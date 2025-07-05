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

package org.springframework.batch.extensions.s3.serializer;


import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class S3StringDeserializerTests {

	@Test
	void testDeserializeSingleLine() {
		S3StringDeserializer deserializer = new S3StringDeserializer();
		String input = "testString\n";
		String result = deserializer.deserialize(input.getBytes(StandardCharsets.UTF_8));
		assertThat(result).isEqualTo("testString");
	}

	@Test
	void testDeserializeMultipleLines() {
		S3StringDeserializer deserializer = new S3StringDeserializer();
		String input = "line1\nline2\n";
		String result1 = deserializer.deserialize(input.getBytes(StandardCharsets.UTF_8));
		assertThat(result1).isEqualTo("line1");
		String result2 = deserializer.deserialize(new byte[0]);
		assertThat(result2).isEqualTo("line2");
	}

	@Test
	void testDeserializeWithCarriageReturn() {
		S3StringDeserializer deserializer = new S3StringDeserializer();
		String input = "line1\r\n";
		String result = deserializer.deserialize(input.getBytes(StandardCharsets.UTF_8));
		assertThat(result).isEqualTo("line1");
	}

	@Test
	void testDeserializePartialInput() {
		S3StringDeserializer deserializer = new S3StringDeserializer();
		String part1 = "partial";
		String part2 = "Line\n";
		assertThat(deserializer.deserialize(part1.getBytes(StandardCharsets.UTF_8))).isNull();
		String result = deserializer.deserialize(part2.getBytes(StandardCharsets.UTF_8));
		assertThat(result).isEqualTo("partialLine");
	}

	@Test
	void testDeserializeEmptyInput() {
		S3StringDeserializer deserializer = new S3StringDeserializer();
		assertThat(deserializer.deserialize("".getBytes(StandardCharsets.UTF_8))).isNull();
	}

}
