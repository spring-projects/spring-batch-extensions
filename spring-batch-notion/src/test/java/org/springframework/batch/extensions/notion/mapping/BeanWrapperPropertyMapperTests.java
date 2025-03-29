/*
 * Copyright 2024-2025 the original author or authors.
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
class BeanWrapperPropertyMapperTests {

	@ParameterizedTest
	@AllPropertiesSource
	void should_map_all_properties(Map<String, String> properties) {
		// GIVEN
		PropertyMapper<TestBean> underTest = new BeanWrapperPropertyMapper<>(TestBean.class);
		// WHEN
		TestBean result = underTest.map(properties);
		// THEN
		then(result) //
			.returns("Value1", from(TestBean::getField1)) //
			.returns("Value2", from(TestBean::getField2));
	}

	@ParameterizedTest
	@PartialPropertiesSource
	void should_map_partial_properties(Map<String, String> properties) {
		// GIVEN
		PropertyMapper<TestBean> underTest = new BeanWrapperPropertyMapper<>(TestBean.class);
		// WHEN
		TestBean result = underTest.map(properties);
		// THEN
		then(result) //
			.returns("Value1", from(TestBean::getField1)) //
			.returns(null, from(TestBean::getField2));
	}

	@ParameterizedTest
	@AllPropertiesSource
	void should_map_all_properties_without_type_parameter(Map<String, String> properties) {
		// GIVEN
		PropertyMapper<TestBean> underTest = new BeanWrapperPropertyMapper<>();
		// WHEN
		TestBean result = underTest.map(properties);
		// THEN
		then(result) //
			.returns("Value1", from(TestBean::getField1)) //
			.returns("Value2", from(TestBean::getField2));
	}

	@Test
	void should_fail_with_vararg_constructor_parameter() {
		// WHEN
		Throwable thrown = catchThrowable(() -> new BeanWrapperPropertyMapper<>(new TestBean()));
		// THEN
		then(thrown) //
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("Please don't pass any values here. The type will be detected automagically.");
	}

	private static class TestBean {

		private String field1;

		private String field2;

		public String getField1() {
			return field1;
		}

		@SuppressWarnings("unused")
		public void setField1(String field1) {
			this.field1 = field1;
		}

		public String getField2() {
			return field2;
		}

		@SuppressWarnings("unused")
		public void setField2(String field2) {
			this.field2 = field2;
		}

	}

}
