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

package org.springframework.batch.extensions.bigquery.reader.builder;

import com.google.cloud.bigquery.FieldValueList;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.SimpleTypeConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.lang.reflect.Constructor;

/**
 * A helper class which tries to convert BigQuery response to a Java record.
 *
 * @param <T> Java record type
 * @author Volodymyr Perebykivskyi
 * @since 0.2.0
 */
public final class RecordMapper<T> {

	private final SimpleTypeConverter simpleConverter = new SimpleTypeConverter();

	/**
	 * Generates a conversion from BigQuery response to a Java record.
	 * @param targetType a Java record {@link Class}
	 * @return {@link Converter}
	 * @see org.springframework.batch.item.file.mapping.RecordFieldSetMapper
	 */
	public Converter<FieldValueList, T> generateMapper(Class<T> targetType) {
		Constructor<T> constructor = BeanUtils.getResolvableConstructor(targetType);
		Assert.isTrue(constructor.getParameterCount() > 0, "Record without fields is redundant");

		String[] parameterNames = BeanUtils.getParameterNames(constructor);
		Class<?>[] parameterTypes = constructor.getParameterTypes();

		Object[] args = new Object[parameterNames.length];

		return source -> {
			if (args[0] == null) {
				for (int i = 0; i < args.length; i++) {
					args[i] = simpleConverter.convertIfNecessary(source.get(parameterNames[i]).getValue(),
							parameterTypes[i]);
				}
			}

			return BeanUtils.instantiateClass(constructor, args);
		};
	}

}
