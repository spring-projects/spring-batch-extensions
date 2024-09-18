/*
 * Copyright 2011-2024 the original author or authors.
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

import org.apache.poi.ss.usermodel.DataFormatter;

/**
 * Callback for customizing a given {@code DataFormatter}. Designed for use with a lambda expression or method reference.
 * @author Marten Deinum
 * @since 0.2.0
 *
 */
@FunctionalInterface
public interface DataFormatterCustomizer {

	/** Noop {@code DataFormatterCustomizer}. **/
	DataFormatterCustomizer NOOP = (df) -> { };

	/** The default {@code DataFormatterCustomizer}, setting the use of cached values. **/
	DataFormatterCustomizer DEFAULT = (df) -> df.setUseCachedValuesForFormulaCells(true);

	void customize(DataFormatter dataFormatter);


}


