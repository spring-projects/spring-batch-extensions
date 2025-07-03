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

package org.springframework.batch.extensions.bigquery.unit.reader.builder;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.extensions.bigquery.common.PersonDto;
import org.springframework.batch.extensions.bigquery.common.TestConstants;
import org.springframework.batch.extensions.bigquery.reader.builder.RecordMapper;
import org.springframework.core.convert.converter.Converter;

import java.util.List;

class RecordMapperTest {

	@Test
	void testGenerateMapper() {
		RecordMapper<PersonDto> mapper = new RecordMapper<>();
		List<PersonDto> expected = TestConstants.CHUNK.getItems();

		Field name = Field.of(TestConstants.NAME, StandardSQLTypeName.STRING);
		Field age = Field.of(TestConstants.AGE, StandardSQLTypeName.INT64);

		PersonDto person1 = expected.get(0);
		FieldValue value1 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.name());
		FieldValue value2 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, person1.age());

		FieldValueList row = FieldValueList.of(List.of(value1, value2), name, age);

		Converter<FieldValueList, PersonDto> converter = mapper.generateMapper(PersonDto.class);
		Assertions.assertNotNull(converter);

		PersonDto actual = converter.convert(row);

		Assertions.assertEquals(expected.get(0).name(), actual.name());
		Assertions.assertEquals(expected.get(0).age(), actual.age());
	}

	@Test
	void testGenerateMapper_EmptyRecord() {
		record TestRecord() {
		}
		IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
				() -> new RecordMapper<TestRecord>().generateMapper(TestRecord.class));
		Assertions.assertEquals("Record without fields is redundant", ex.getMessage());
	}

}
