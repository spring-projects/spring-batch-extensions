/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.batch.extensions.kotlindsl.job

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.SimpleJobBuilder
import org.springframework.batch.extensions.kotlindsl.step.StepBuilderDsl
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext

/**
 * A dsl for [SimpleJobBuilder][org.springframework.batch.core.job.builder.SimpleJobBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class SimpleJobBuilderDsl internal constructor(
    private val dslContext: DslContext,
    private val simpleJobBuilder: SimpleJobBuilder
) {
    /**
     * Add step by bean name.
     */
    fun stepBean(name: String) {
        val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
        step(step)
    }

    /**
     * Add step.
     */
    fun step(name: String, init: StepBuilderDsl.() -> Step) {
        val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
        val step = StepBuilderDsl(this.dslContext, stepBuilder).let(init)
        step(step)
    }

    /**
     * Add step.
     */
    fun step(step: Step) {
        this.simpleJobBuilder.next(step)
    }

    internal fun build(): Job = this.simpleJobBuilder.build()
}
