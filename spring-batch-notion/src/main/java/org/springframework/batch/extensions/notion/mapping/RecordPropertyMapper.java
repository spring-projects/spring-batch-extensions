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

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;

/**
 * {@link PropertyMapper} implementation for {@link Record Java records}.
 * <p>
 * It uses the record's canonical constructor and requires the record's component names to
 * match the Notion item property names (case-insensitive).
 *
 * @author Stefano Cordio
 * @param <T> the target type &mdash; must be a {@link Record}
 */
public class RecordPropertyMapper<T extends Record> extends ConstructorBasedPropertyMapper<T> {

	/**
	 * Create a new {@link RecordPropertyMapper} for the given target type.
	 * @param type type of the target record
	 */
	public RecordPropertyMapper(Class<T> type) {
		super(type);
	}

	/**
	 * Create a new {@link RecordPropertyMapper}, inferring the target type.
	 * @param reified don't pass any values to it. It's a trick to detect the target type.
	 */
	@SafeVarargs
	public RecordPropertyMapper(T... reified) {
		this(ClassResolver.getClassOf(reified));
	}

	@Override
	Constructor<T> getConstructor(Class<T> type) throws NoSuchMethodException {
		Class<?>[] parameterTypes = Arrays.stream(type.getRecordComponents()) //
			.map(RecordComponent::getType) //
			.toArray(Class[]::new);

		return ReflectionUtils.accessibleConstructor(type, parameterTypes);
	}

}
