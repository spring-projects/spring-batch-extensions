/*
 * Copyright 2024-2026 the original author or authors.
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

import org.junit.jupiter.params.provider.FieldSource;

import java.lang.annotation.Retention;
import java.util.List;
import java.util.Map;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author Stefano Cordio
 */
class TestData {

	@Retention(RUNTIME)
	@FieldSource("org.springframework.batch.extensions.notion.mapping.TestData#ALL_PROPERTIES")
	@interface AllPropertiesSource {

	}

	static final List<Map<String, String>> ALL_PROPERTIES = List.of(
			// FIXME not working with BeanWrapperPropertyMapper
			// Map.of("FIELD1", "Value1", "FIELD2", "Value2"), //
			Map.of("Field1", "Value1", "Field2", "Value2"), //
			Map.of("field1", "Value1", "field2", "Value2"));

	@Retention(RUNTIME)
	@FieldSource("org.springframework.batch.extensions.notion.mapping.TestData#PARTIAL_PROPERTIES")
	@interface PartialPropertiesSource {

	}

	static final List<Map<String, String>> PARTIAL_PROPERTIES = List.of(
			// FIXME not working with BeanWrapperPropertyMapper
			// Map.of("FIELD1", "Value1"), //
			Map.of("Field1", "Value1"), //
			Map.of("field1", "Value1"));

}
