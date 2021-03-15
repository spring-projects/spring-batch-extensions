/*
 * Copyright 2006-2021 the original author or authors.
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

package org.springframework.batch.extensions.excel;

import java.lang.reflect.Field;

import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

/**
 * Simplified version of {@code ReflectionTestUtils} from Spring. This to prevent a
 * unneeded dependency on the Spring Test module.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
public final class ReflectionTestUtils {

	private ReflectionTestUtils() { }

	@Nullable
	public static Object getField(Object targetObject, String name) {
		Class<?> targetClass = targetObject.getClass();

		Field field = ReflectionUtils.findField(targetClass, name);
		if (field == null) {
			throw new IllegalArgumentException(String.format("Could not find field '%s' on %s or target class [%s]",
					name, targetObject, targetClass));
		}

		ReflectionUtils.makeAccessible(field);
		return ReflectionUtils.getField(field, targetObject);
	}

}
