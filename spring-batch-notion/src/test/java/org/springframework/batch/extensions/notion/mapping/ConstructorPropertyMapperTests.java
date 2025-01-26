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
package org.springframework.batch.extensions.notion.mapping;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.batch.extensions.notion.mapping.TestData.AllPropertiesSource;
import org.springframework.batch.extensions.notion.mapping.TestData.PartialPropertiesSource;

import java.util.Map;

import static org.assertj.core.api.BDDAssertions.catchThrowable;
import static org.assertj.core.api.BDDAssertions.from;
import static org.assertj.core.api.BDDAssertions.then;

/**
 * @author Stefano Cordio
 */
class ConstructorPropertyMapperTests {

	@Nested
	class using_record_without_additional_constructors {

		private record TestRecord(String field1, String field2) {
		}

		@ParameterizedTest
		@AllPropertiesSource
		void should_map_all_properties(Map<String, String> properties) {
			// GIVEN
			PropertyMapper<TestRecord> underTest = new ConstructorPropertyMapper<>(TestRecord.class);
			// WHEN
			TestRecord result = underTest.map(properties);
			// THEN
			then(result) //
				.returns("Value1", from(TestRecord::field1)) //
				.returns("Value2", from(TestRecord::field2));
		}

		@ParameterizedTest
		@AllPropertiesSource
		void should_map_all_properties_without_type_parameter(Map<String, String> properties) {
			// GIVEN
			PropertyMapper<TestRecord> underTest = new ConstructorPropertyMapper<>();
			// WHEN
			TestRecord result = underTest.map(properties);
			// THEN
			then(result) //
				.returns("Value1", from(TestRecord::field1)) //
				.returns("Value2", from(TestRecord::field2));
		}

		@Test
		void should_fail_with_vararg_constructor_parameter() {
			// WHEN
			Throwable thrown = catchThrowable(() -> new ConstructorPropertyMapper<>(new TestRecord("value", "value")));
			// THEN
			then(thrown) //
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Please don't pass any values here. The type will be detected automagically.");
		}

		@ParameterizedTest
		@PartialPropertiesSource
		void should_map_partial_properties(Map<String, String> properties) {
			// GIVEN
			PropertyMapper<TestRecord> underTest = new ConstructorPropertyMapper<>(TestRecord.class);
			// WHEN
			TestRecord result = underTest.map(properties);
			// THEN
			then(result) //
				.returns("Value1", from(TestRecord::field1)) //
				.returns(null, from(TestRecord::field2));
		}

	}

	@Nested
	class using_record_with_additional_constructors {

		private record TestRecord(String field1, String field2) {

			@SuppressWarnings("unused")
			private TestRecord() {
				this(null, null);
			}

			@SuppressWarnings("unused")
			private TestRecord(String field1) {
				this(field1, null);
			}

		}

		@Test
		void should_fail() {
			// WHEN
			Throwable thrown = catchThrowable(() -> new ConstructorPropertyMapper<>(TestRecord.class));
			// THEN
			then(thrown).isInstanceOf(IllegalArgumentException.class) //
				.cause() //
				.isInstanceOf(NoSuchMethodException.class)
				.hasMessageStartingWith("Multiple constructors available: ");
		}

	}

}
