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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Simple deserializer for String items from S3. It reads lines from a byte array,
 * handling both \n and \r\n line endings.
 *
 * This is intended to be used with S3ItemReader to read text data from S3 objects.
 *
 * @author Andrea Cioni
 */
public class S3StringDeserializer implements S3Deserializer<String> {

	final Charset charset;

	private StringBuilder stringBuilder = new StringBuilder();

	public S3StringDeserializer() {
		this.charset = StandardCharsets.UTF_8;
	}

	public S3StringDeserializer(Charset charset) {
		this.charset = charset;
	}

	@Override
	public String deserialize(byte[] buffer) {
		String incoming = new String(buffer, this.charset);
		this.stringBuilder.append(incoming);

		int newlineIdx = this.stringBuilder.indexOf("\n");
		if (newlineIdx == -1) {
			return null;
		}

		// Handle both \n and \r\n line endings
		int lineEnd = newlineIdx;
		if (newlineIdx > 0 && this.stringBuilder.charAt(newlineIdx - 1) == '\r') {
			lineEnd--;
		}

		String line = this.stringBuilder.substring(0, lineEnd);
		this.stringBuilder = new StringBuilder(this.stringBuilder.substring(newlineIdx + 1));

		return line;
	}

}
