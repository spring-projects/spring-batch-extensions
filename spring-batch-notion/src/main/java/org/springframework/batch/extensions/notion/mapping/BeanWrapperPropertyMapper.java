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

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.lang.reflect.Constructor;

/**
 * {@link PropertyMapper} implementation for JavaBeans.
 * <p>
 * It requires a default constructor and expects the setter names to match the Notion item
 * property names (case-insensitive).
 *
 * @author Stefano Cordio
 * @param <T> the target type
 */
public class BeanWrapperPropertyMapper<T> extends CaseInsensitivePropertyMapper<T> {

	private final Constructor<T> constructor;

	/**
	 * Create a new {@link BeanWrapperPropertyMapper} for the given target type.
	 * @param type type of the target object
	 */
	public BeanWrapperPropertyMapper(Class<T> type) {
		this.constructor = BeanUtils.getResolvableConstructor(type);
	}

	/**
	 * Create a new {@link BeanWrapperPropertyMapper}, inferring the target type.
	 * @param reified don't pass any values to it. It's a trick to detect the target type.
	 */
	@SafeVarargs
	public BeanWrapperPropertyMapper(T... reified) {
		this(ClassResolver.getClassOf(reified));
	}

	@Override
	T mapCaseInsensitive(LinkedCaseInsensitiveMap<String> properties) {
		T instance = BeanUtils.instantiateClass(constructor);
		BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(instance);
		beanWrapper.setPropertyValues(properties);
		return instance;
	}

}
