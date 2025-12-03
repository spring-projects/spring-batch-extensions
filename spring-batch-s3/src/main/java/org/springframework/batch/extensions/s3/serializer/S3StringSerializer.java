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

package org.springframework.batch.extensions.s3.serializer;

import java.nio.charset.StandardCharsets;

/**
 * Simple serializer for String items to be used with S3. This serializer takes a String
 * item, appends a newline character, and converts it to a byte array using UTF-8
 * encoding. This is intended to be used with S3ItemWriter to write text data to S3
 * objects.
 *
 * @author Andrea Cioni
 */
public class S3StringSerializer implements S3Serializer<String> {

	@Override
	public byte[] serialize(String item) {
		return (item + "\n").getBytes(StandardCharsets.UTF_8);
	}

}
