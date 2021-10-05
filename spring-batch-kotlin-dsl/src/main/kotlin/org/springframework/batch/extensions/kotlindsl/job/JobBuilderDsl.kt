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
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.JobParametersIncrementer
import org.springframework.batch.core.JobParametersValidator
import org.springframework.batch.core.job.builder.FlowJobBuilder
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.builder.SimpleJobBuilder
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext

/**
 * A dsl for [JobBuilder][org.springframework.batch.core.job.builder.JobBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class JobBuilderDsl internal constructor(
    private val dslContext: DslContext,
    private val jobBuilder: JobBuilder
) {
    /**
     * Set for [JobBuilder.validator][org.springframework.batch.core.job.builder.JobBuilderHelper.validator].
     */
    fun validator(jobParametersValidator: JobParametersValidator) {
        this.jobBuilder.validator(jobParametersValidator)
    }

    /**
     * Set for [JobBuilder.incrementer][org.springframework.batch.core.job.builder.JobBuilderHelper.incrementer].
     */
    fun incrementer(jobParametersIncrementer: JobParametersIncrementer) {
        this.jobBuilder.incrementer(jobParametersIncrementer)
    }

    /**
     * Set for [JobBuilder.repository][org.springframework.batch.core.job.builder.JobBuilderHelper.repository].
     */
    fun repository(jobRepository: JobRepository) {
        this.jobBuilder.repository(jobRepository)
    }

    /**
     * Set listener processing followings.
     *
     * - [org.springframework.batch.core.annotation.BeforeJob]
     * - [org.springframework.batch.core.annotation.AfterJob]
     */
    fun listener(listener: Any) {
        this.jobBuilder.listener(listener)
    }

    /**
     * Set job execution listener.
     */
    fun listener(listener: JobExecutionListener) {
        this.jobBuilder.listener(listener)
    }

    /**
     * Set for [JobBuilder.preventRestart][org.springframework.batch.core.job.builder.JobBuilderHelper.preventRestart].
     */
    fun preventRestart() {
        this.jobBuilder.preventRestart()
    }

    /**
     * Build [SimpleJobBuilder][org.springframework.batch.core.job.builder.SimpleJobBuilder] for job.
     */
    fun steps(init: SimpleJobBuilderDsl.() -> Unit): Job {
        val simpleJobBuilder = SimpleJobBuilder(this.jobBuilder)
        return SimpleJobBuilderDsl(this.dslContext, simpleJobBuilder).apply(init).build()
    }

    /**
     * Build [FlowJobBuilder][org.springframework.batch.core.job.builder.FlowJobBuilder] for job.
     */
    fun flows(init: FlowJobBuilderDsl.() -> Unit): Job {
        val flowJobBuilder = FlowJobBuilder(this.jobBuilder)
        return FlowJobBuilderDsl(this.dslContext, flowJobBuilder).apply(init).build().build()
    }
}
