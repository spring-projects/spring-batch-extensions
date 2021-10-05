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
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.support.transaction.ResourcelessTransactionManager

/**
 * @author Taeik Lim
 */
internal class FlowStepBuilderDslTests {

    private val jobInstance = JobInstance(0L, "testJob")

    private val jobParameters = JobParameters()

    @Test
    fun testBuild() {
        // given
        var firstStepCallCount = 0
        var secondStepCallCount = 0

        // when
        val flow = FlowBuilder<Flow>("testFlow")
            .start(
                object : Step {
                    override fun getName(): String {
                        return "step1"
                    }

                    override fun isAllowStartIfComplete(): Boolean {
                        return false
                    }

                    override fun getStartLimit(): Int {
                        return 1
                    }

                    override fun execute(stepExecution: StepExecution) {
                        ++firstStepCallCount
                        stepExecution.apply {
                            status = BatchStatus.COMPLETED
                            exitStatus = ExitStatus.COMPLETED
                        }
                    }
                }
            )
            .next(
                object : Step {
                    override fun getName(): String {
                        return "step2"
                    }

                    override fun isAllowStartIfComplete(): Boolean {
                        return false
                    }

                    override fun getStartLimit(): Int {
                        return 1
                    }

                    override fun execute(stepExecution: StepExecution) {
                        ++secondStepCallCount
                        stepExecution.apply {
                            status = BatchStatus.COMPLETED
                            exitStatus = ExitStatus.COMPLETED
                        }
                    }
                }
            )
            .build()
        val step = flowStepBuilderDsl(flow)
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(firstStepCallCount).isEqualTo(1)
        assertThat(secondStepCallCount).isEqualTo(1)
        assertThat(jobExecution.stepExecutions).hasSize(3)
            .anySatisfy { assertThat(it.stepName).isEqualTo("testStep") }
            .anySatisfy { assertThat(it.stepName).isEqualTo("step1") }
            .anySatisfy { assertThat(it.stepName).isEqualTo("step2") }
    }

    private fun flowStepBuilderDsl(flow: Flow): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val flowStepBuilder = stepBuilder.flow(flow)

        return FlowStepBuilderDsl(dslContext, flowStepBuilder).build()
    }
}
