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

import org.springframework.beans.BeanUtils;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.util.Arrays;

/**
 * @author Stefano Cordio
 */
abstract class ConstructorBasedPropertyMapper<T> extends CaseInsensitivePropertyMapper<T> {

	private final Constructor<T> constructor;

	ConstructorBasedPropertyMapper(Class<T> type) {
		try {
			this.constructor = getConstructor(type);
		}
		catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		}
	}

	abstract Constructor<T> getConstructor(Class<T> type) throws NoSuchMethodException;

	@Override
	T mapCaseInsensitive(LinkedCaseInsensitiveMap<String> properties) {
		Object[] parameterValues = Arrays.stream(constructor.getParameters()) //
			.map(Parameter::getName) //
			.map(properties::get) //
			.toArray();

		return BeanUtils.instantiateClass(constructor, parameterValues);

	}

}
