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

package org.springframework.batch.extensions.kotlindsl.configuration

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.extensions.kotlindsl.flow.FlowBuilderDsl
import org.springframework.batch.extensions.kotlindsl.job.JobBuilderDsl
import org.springframework.batch.extensions.kotlindsl.step.StepBuilderDsl
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.beans.factory.BeanFactory

/**
 * A dsl for spring batch job, step, flow.
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class BatchDsl internal constructor(
    private val dslContext: DslContext,
) {
    constructor(
        beanFactory: BeanFactory,
        jobBuilderFactory: JobBuilderFactory,
        stepBuilderFactory: StepBuilderFactory
    ) : this(
        DslContext(
            beanFactory,
            jobBuilderFactory,
            stepBuilderFactory
        )
    )

    operator fun <T : Any> invoke(init: BatchDsl.() -> T): T = init()

    fun job(name: String, init: JobBuilderDsl.() -> Job): Job {
        val jobBuilderFactory = this.dslContext.jobBuilderFactory
        return JobBuilderDsl(this.dslContext, jobBuilderFactory.get(name)).let(init)
    }

    fun step(name: String, init: StepBuilderDsl.() -> Step): Step {
        val stepBuilderFactory = this.dslContext.stepBuilderFactory
        return StepBuilderDsl(this.dslContext, stepBuilderFactory.get(name)).let(init)
    }

    fun flow(name: String, init: FlowBuilderDsl<Flow>.() -> Unit): Flow {
        return FlowBuilderDsl(this.dslContext, FlowBuilder<Flow>(name)).apply(init).build()
    }
}
