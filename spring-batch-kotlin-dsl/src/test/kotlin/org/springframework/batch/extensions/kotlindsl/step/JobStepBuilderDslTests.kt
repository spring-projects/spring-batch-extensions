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

package org.springframework.batch.extensions.kotlindsl.step

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.support.transaction.ResourcelessTransactionManager

/**
 * @author Taeik Lim
 */
internal class JobStepBuilderDslTests {

    private val jobInstance = JobInstance(0L, "testJob")

    private val jobParameters = JobParameters()

    @Test
    fun testLauncher() {
        // given
        var jobLauncherCallCount = 0

        // when
        val job: Job = mock()
        val step = jobStepBuilderDsl(job) {
            launcher { _, _ ->
                ++jobLauncherCallCount
                JobExecution(jobInstance, jobParameters)
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobLauncherCallCount).isEqualTo(1)
    }

    @Test
    fun testParametersExtractor() {
        // given
        var parameterExtractorCallCount = 0

        // when
        val job: Job = mock()
        val step = jobStepBuilderDsl(job) {
            launcher { _, _ ->
                JobExecution(jobInstance, jobParameters)
            }
            parametersExtractor { _, _ ->
                ++parameterExtractorCallCount
                JobParameters()
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(parameterExtractorCallCount).isEqualTo(1)
    }

    private fun jobStepBuilderDsl(job: Job, init: JobStepBuilderDsl.() -> Unit): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val jobStepBuilder = stepBuilder.job(job)

        return JobStepBuilderDsl(dslContext, jobStepBuilder).apply(init).build()
    }
}
