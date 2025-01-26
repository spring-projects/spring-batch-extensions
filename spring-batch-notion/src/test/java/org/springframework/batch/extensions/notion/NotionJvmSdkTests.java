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
package org.springframework.batch.extensions.notion;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.base.DescribedPredicate.anyElementThat;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

/**
 * @author Stefano Cordio
 */
@AnalyzeClasses(packagesOf = NotionDatabaseItemReader.class)
class NotionJvmSdkTests {

	private static final DescribedPredicate<JavaClass> RESIDE_IN_NOTION_JVM_SDK_PACKAGE = //
			resideInAPackage("notion.api..");

	@ArchTest
	void library_types_should_not_be_exposed(JavaClasses classes) {
		// @formatter:off
		ArchRule rule = methods()
				.that().arePublic().or().areProtected()
				.should().notHaveRawReturnType(RESIDE_IN_NOTION_JVM_SDK_PACKAGE)
				.andShould().notHaveRawParameterTypes(anyElementThat(RESIDE_IN_NOTION_JVM_SDK_PACKAGE));
		// @formatter:on
		rule.check(classes);
	}

}
