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

import java.util.Map;

/**
 * Strategy interface for mapping the properties of a Notion item into a Java object.
 *
 * @author Stefano Cordio
 * @param <T> the object type
 */
@FunctionalInterface
public interface PropertyMapper<T> {

	/**
	 * Map the given item properties into an object of type {@code T}.
	 * @param properties unmodifiable map containing the property value objects, keyed by
	 * property name
	 * @return the populated object
	 */
	T map(Map<String, String> properties);

}
