/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.batch.extensions.bigquery.common;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import org.junit.jupiter.api.Assertions;
import org.springframework.batch.item.Chunk;

import java.util.List;

public final class ResultVerifier {

	private ResultVerifier() {
	}

	public static void verifyTableResult(Chunk<PersonDto> expected, TableResult actual) {
		List<FieldValueList> actualList = actual.streamValues().toList();

		Assertions.assertEquals(expected.size(), actual.getTotalRows());
		Assertions.assertEquals(expected.size(), actualList.size());

		actualList.forEach(field -> {
			boolean containsName = expected.getItems()
				.stream()
				.map(PersonDto::name)
				.anyMatch(name -> field.get(0).getStringValue().equals(name));

			boolean containsAge = expected.getItems()
				.stream()
				.map(PersonDto::age)
				.map(Long::valueOf)
				.anyMatch(age -> age.compareTo(field.get(1).getLongValue()) == 0);

			Assertions.assertTrue(containsName);
			Assertions.assertTrue(containsAge);
		});
	}

}
