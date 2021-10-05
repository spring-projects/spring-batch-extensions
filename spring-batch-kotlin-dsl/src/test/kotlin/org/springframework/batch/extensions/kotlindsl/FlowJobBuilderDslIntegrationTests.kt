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

package org.springframework.batch.extensions.kotlindsl

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.extensions.kotlindsl.configuration.BatchDsl
import org.springframework.batch.extensions.kotlindsl.configuration.EnableBatchDsl
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * @author Taeik Lim
 */
internal class FlowJobBuilderDslIntegrationTests {

    @Test
    fun testStepBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val testStep1 = batch {
            step("testStep1") {
                tasklet { _, _ ->
                    ++testStep1CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        val testStep2 = batch {
            step("testStep2") {
                tasklet { _, _ ->
                    ++testStep2CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        context.apply {
            registerBean("testStep1", Step::class.java, Supplier { testStep1 })
            registerBean("testStep2", Step::class.java, Supplier { testStep2 })
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        stepBean("testStep1")
                        stepBean("testStep2")
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testStepWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step("testStep1") {
                            tasklet { _, _ ->
                                ++testStep1CallCount
                                RepeatStatus.FINISHED
                            }
                        }
                        step("testStep2") {
                            tasklet { _, _ ->
                                ++testStep2CallCount
                                RepeatStatus.FINISHED
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testStepByVariable() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val testStep1 = batch {
            step("testStep1") {
                tasklet { _, _ ->
                    ++testStep1CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        val testStep2 = batch {
            step("testStep2") {
                tasklet { _, _ ->
                    ++testStep2CallCount
                    RepeatStatus.FINISHED
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step(testStep1)
                        step(testStep2)
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testStepBeanWithTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0
        val testStep1 = batch {
            step("testStep1") {
                tasklet { _, _ ->
                    ++testStep1CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        val testStep2 = batch {
            step("testStep2") {
                tasklet { _, _ ->
                    ++testStep2CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        context.apply {
            registerBean("testStep1", Step::class.java, Supplier { testStep1 })
            registerBean("testStep2", Step::class.java, Supplier { testStep2 })
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        stepBean("testStep1") {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        stepBean("testStep2") {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testStepWithInitAndTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step(
                            "testStep1",
                            {
                                tasklet { _, _ ->
                                    ++testStep1CallCount
                                    RepeatStatus.FINISHED
                                }
                            }
                        ) {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        step(
                            "testStep2",
                            {
                                tasklet { _, _ ->
                                    ++testStep2CallCount
                                    RepeatStatus.FINISHED
                                }
                            }
                        ) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testStepWithVariableAndTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0
        val testStep1 = batch {
            step("testStep1") {
                tasklet { _, _ ->
                    ++testStep1CallCount
                    RepeatStatus.FINISHED
                }
            }
        }
        val testStep2 = batch {
            step("testStep2") {
                tasklet { _, _ ->
                    ++testStep2CallCount
                    RepeatStatus.FINISHED
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step(testStep1) {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        step(testStep2) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val testFlow1 = batch {
            flow("testFlow1") {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        val testFlow2 = batch {
            flow("testFlow2") {
                step("testStep2") {
                    tasklet { _, _ ->
                        ++testStep2CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        context.apply {
            registerBean("testFlow1", Flow::class.java, Supplier { testFlow1 })
            registerBean("testFlow2", Flow::class.java, Supplier { testFlow2 })
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flowBean("testFlow1")
                        flowBean("testFlow2")
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flow("testFlow1") {
                            step("testStep1") {
                                tasklet { _, _ ->
                                    ++testStep1CallCount
                                    RepeatStatus.FINISHED
                                }
                            }
                        }
                        flow("testFlow2") {
                            step("testStep2") {
                                tasklet { _, _ ->
                                    ++testStep2CallCount
                                    RepeatStatus.FINISHED
                                }
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowByVariable() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val testFlow1 = batch {
            flow("testFlow1") {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        val testFlow2 = batch {
            flow("testFlow2") {
                step("testStep2") {
                    tasklet { _, _ ->
                        ++testStep2CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flow(testFlow1)
                        flow(testFlow2)
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowBeanWithTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0
        val testFlow1 = batch {
            flow("testFlow1") {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        val testFlow2 = batch {
            flow("testFlow2") {
                step("testStep2") {
                    tasklet { _, _ ->
                        ++testStep2CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        context.apply {
            registerBean("testFlow1", Flow::class.java, Supplier { testFlow1 })
            registerBean("testFlow2", Flow::class.java, Supplier { testFlow2 })
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flowBean("testFlow1") {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        flowBean("testFlow2") {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowWithInitAndTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flow(
                            "testFlow1",
                            {
                                step("testStep1") {
                                    tasklet { _, _ ->
                                        ++testStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        ) {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        flow(
                            "testFlow2",
                            {
                                step("testStep2") {
                                    tasklet { _, _ ->
                                        ++testStep2CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        ) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowWithVariableAndTransition() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var transitionStep1CallCount = 0
        var testStep2CallCount = 0
        val testFlow1 = batch {
            flow("testFlow1") {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }
        val testFlow2 = batch {
            flow("testFlow2") {
                step("testStep2") {
                    tasklet { _, _ ->
                        ++testStep2CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        flow(testFlow1) {
                            on("COMPLETED") {
                                step("transitionStep1") {
                                    tasklet { _, _ ->
                                        ++transitionStep1CallCount
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                        flow(testFlow2) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(transitionStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testDeciderBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testDeciderCallCount = 0
        val testDecider = JobExecutionDecider { _, _ ->
            ++testDeciderCallCount
            FlowExecutionStatus.COMPLETED
        }
        context.registerBean("testDecider", JobExecutionDecider::class.java, Supplier { testDecider })

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        deciderBean("testDecider") {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testDeciderCallCount).isEqualTo(1)
    }

    @Test
    fun testDeciderBeanNotFirst() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testDeciderCallCount = 0
        val testDecider = JobExecutionDecider { _, _ ->
            ++testDeciderCallCount
            FlowExecutionStatus.COMPLETED
        }
        context.registerBean("testDecider", JobExecutionDecider::class.java, Supplier { testDecider })

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step("testStep1") {
                            tasklet { _, _ ->
                                ++testStep1CallCount
                                RepeatStatus.FINISHED
                            }
                        }
                        deciderBean("testDecider") {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testDeciderCallCount).isEqualTo(1)
    }

    @Test
    fun testDeciderByVariable() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testDeciderCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        decider(
                            { _, _ ->
                                ++testDeciderCallCount
                                FlowExecutionStatus.COMPLETED
                            }
                        ) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testDeciderCallCount).isEqualTo(1)
    }

    @Test
    fun testDeciderByVariableNotFirst() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testDeciderCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    flow("testFlow") {
                        step("testStep1") {
                            tasklet { _, _ ->
                                ++testStep1CallCount
                                RepeatStatus.FINISHED
                            }
                        }
                        decider(
                            { _, _ ->
                                ++testDeciderCallCount
                                FlowExecutionStatus.COMPLETED
                            }
                        ) {
                            on("COMPLETED") {
                                end("TEST")
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testDeciderCallCount).isEqualTo(1)
    }

    @Test
    fun testSplit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var taskExecutorCallCount = 0
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val callerThread = Thread.currentThread().name
        val taskExecutor = object : ThreadPoolTaskExecutor() {
            override fun execute(task: Runnable) {
                ++taskExecutorCallCount
                super.execute(task)
            }
        }.apply { initialize() }

        // when
        val job = batch {
            job("testJob") {
                flows {
                    split(taskExecutor) {
                        flow("testFlow1") {
                            step("testStep1") {
                                tasklet { _, _ ->
                                    ++testStep1CallCount
                                    assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                                    RepeatStatus.FINISHED
                                }
                            }
                        }
                        flow("testFlow2") {
                            step("testStep2") {
                                tasklet { _, _ ->
                                    ++testStep2CallCount
                                    assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                                    RepeatStatus.FINISHED
                                }
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
        assertThat(taskExecutorCallCount).isEqualTo(2)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Nested
    inner class StepTransitionBuilderDslTests {

        @RepeatedTest(10)
        @Test
        fun testStepWithMultipleTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val expectedExitStatus = randomExitStatus()
            var testStep1CallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { contribution, _ ->
                        ++testStep1CallCount
                        contribution.exitStatus = expectedExitStatus
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    end()
                                }
                                on("*") {
                                    fail()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(testStep1CallCount).isEqualTo(1)
            when (expectedExitStatus) {
                ExitStatus.COMPLETED -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
                }
                else -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
                }
            }
        }

        @Test
        fun testStepWithNoTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when, then
            assertThatThrownBy {
                batch {
                    job("testJob") {
                        flows {
                            flow("testFlow") {
                                step(testStep1) {
                                    // no transition
                                }
                            }
                        }
                    }
                }
            }.hasMessageContaining("should set transition for step")
        }
    }

    @Nested
    inner class FlowTransitionBuilderDslTests {

        @RepeatedTest(10)
        @Test
        fun testFlowWithMultipleTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val expectedExitStatus = randomExitStatus()
            var testStep1CallCount = 0
            val testFlow1 = batch {
                flow("testFlow1") {
                    step("testStep1") {
                        tasklet { contribution, _ ->
                            ++testStep1CallCount
                            contribution.exitStatus = expectedExitStatus
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            flow(testFlow1) {
                                on("COMPLETED") {
                                    end()
                                }
                                on("*") {
                                    fail()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(testStep1CallCount).isEqualTo(1)
            when (expectedExitStatus) {
                ExitStatus.COMPLETED -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
                }
                else -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
                }
            }
        }

        @Test
        fun testStepWithNoTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val testFlow1 = batch {
                flow("testFlow1") {
                    step("testStep1") {
                        tasklet { _, _ ->
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when, then
            assertThatThrownBy {
                batch {
                    job("testJob") {
                        flows {
                            flow("testFlow") {
                                flow(testFlow1) {
                                    // no transition
                                }
                            }
                        }
                    }
                }
            }.hasMessageContaining("should set transition for flow")
        }
    }

    @Nested
    inner class DeciderTransitionBuilderDsl {

        @RepeatedTest(10)
        @Test
        fun testDeciderWithMultipleTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val expectedFlowExecutionStatus = randomFlowExecutionStatus()
            var testDeciderCallCount = 0
            val testDecider = JobExecutionDecider { _, _ ->
                ++testDeciderCallCount
                expectedFlowExecutionStatus
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            decider(testDecider) {
                                on("UNKNOWN") {
                                    end()
                                }
                                on("*") {
                                    fail()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(testDeciderCallCount).isEqualTo(1)
            when (expectedFlowExecutionStatus) {
                FlowExecutionStatus.UNKNOWN -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
                    // just finish it so no exit status
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
                }
                FlowExecutionStatus.STOPPED -> { // when stopped, just stop the job
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.STOPPED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
                }
                else -> {
                    assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
                    assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
                }
            }
        }

        @Test
        fun testStepWithNoTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val testDecider = JobExecutionDecider { _, _ ->
                FlowExecutionStatus.COMPLETED
            }

            // when, then
            assertThatThrownBy {
                batch {
                    job("testJob") {
                        flows {
                            flow("testFlow") {
                                decider(testDecider) {
                                    // no transition
                                }
                            }
                        }
                    }
                }
            }.hasMessageContaining("should set transition for decider")
        }
    }

    @Nested
    inner class TransitionBuilderDslTests {

        @Test
        fun testTransitionToStepBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            context.registerBean("transitionStep", Step::class.java, Supplier { transitionStep })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stepBean("transitionStep")
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStepWithInit() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    step("transitionStep") {
                                        tasklet { _, _ ->
                                            ++transitionStepCallCount
                                            RepeatStatus.FINISHED
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStepVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    step(transitionStep)
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStepBeanWithTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            context.registerBean("transitionStep", Step::class.java, Supplier { transitionStep })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stepBean("transitionStep") {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStepWithInitAndTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    step(
                                        "transitionStep",
                                        {
                                            tasklet { _, _ ->
                                                ++transitionStepCallCount
                                                RepeatStatus.FINISHED
                                            }
                                        }
                                    ) {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStepVariableWithTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    step(transitionStep) {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            context.registerBean("transitionFlow", Flow::class.java, Supplier { transitionFlow })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flowBean("transitionFlow")
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowWithInit() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flow("transitionFlow") {
                                        step("transitionStep") {
                                            tasklet { _, _ ->
                                                ++transitionStepCallCount
                                                RepeatStatus.FINISHED
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flow(transitionFlow)
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowBeanWithTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            context.registerBean("transitionFlow", Flow::class.java, Supplier { transitionFlow })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flowBean("transitionFlow") {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowWithInitAndTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flow(
                                        "transitionFlow",
                                        {
                                            step("transitionStep") {
                                                tasklet { _, _ ->
                                                    ++transitionStepCallCount
                                                    RepeatStatus.FINISHED
                                                }
                                            }
                                        }
                                    ) {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFlowWithVariableAndTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    flow(transitionFlow) {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToDeciderBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var testDeciderCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val testDecider = JobExecutionDecider { _, _ ->
                ++testDeciderCallCount
                FlowExecutionStatus.COMPLETED
            }
            context.registerBean("testDecider", JobExecutionDecider::class.java, Supplier { testDecider })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    deciderBean("testDecider") {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testDeciderCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToDeciderVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var testDeciderCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val testDecider = JobExecutionDecider { _, _ ->
                ++testDeciderCallCount
                FlowExecutionStatus.COMPLETED
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    decider(testDecider) {
                                        on("COMPLETED") {
                                            fail()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testDeciderCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStop() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stop()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToFlowBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            context.registerBean("transitionFlow", Flow::class.java, Supplier { transitionFlow })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToFlowBean("transitionFlow")
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToFlowWithInit() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToFlow("transitionFlow") {
                                        step("transitionStep") {
                                            tasklet { _, _ ->
                                                ++transitionStepCallCount
                                                RepeatStatus.FINISHED
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToFlowVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionFlow = batch {
                flow("transitionFlow") {
                    step("transitionStep") {
                        tasklet { _, _ ->
                            ++transitionStepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToFlow(transitionFlow)
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToDeciderBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var testDeciderCallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val testDecider = JobExecutionDecider { _, _ ->
                ++testDeciderCallCount
                FlowExecutionStatus.UNKNOWN
            }
            context.registerBean("testDecider", JobExecutionDecider::class.java, Supplier { testDecider })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToDeciderBean("testDecider") {
                                        on("UNKNOWN") {
                                            step("transitionStep") {
                                                tasklet { _, _ ->
                                                    ++transitionStepCallCount
                                                    RepeatStatus.FINISHED
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testDeciderCallCount).isEqualTo(2) // always run on restart
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToDeciderVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var testDeciderCallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToDecider(
                                        { _, _ ->
                                            ++testDeciderCallCount
                                            FlowExecutionStatus.UNKNOWN
                                        }
                                    ) {
                                        on("UNKNOWN") {
                                            step("transitionStep") {
                                                tasklet { _, _ ->
                                                    ++transitionStepCallCount
                                                    RepeatStatus.FINISHED
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testDeciderCallCount).isEqualTo(2) // always run on restart
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToStepBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            context.registerBean("transitionStep", Step::class.java, Supplier { transitionStep })

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToStepBean("transitionStep")
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToStepWithInit() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToStep("transitionStep") {
                                        tasklet { _, _ ->
                                            ++transitionStepCallCount
                                            RepeatStatus.FINISHED
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToStopAndRestartToStepVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            var transitionStepCallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }
            val transitionStep = batch {
                step("transitionStep") {
                    tasklet { _, _ ->
                        ++transitionStepCallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    stopAndRestartToStep(transitionStep)
                                }
                            }
                        }
                    }
                }
            }
            val firstJobExecution = jobLauncher.run(job, JobParameters())
            val secondJobExecution = jobLauncher.run(job, JobParameters())
            val thirdJobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(firstJobExecution.status).isEqualTo(BatchStatus.STOPPED)
            assertThat(firstJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.STOPPED.exitCode)
            assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(secondJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(thirdJobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.NOOP.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(transitionStepCallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToEnd() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { contribution, _ ->
                        ++testStep1CallCount
                        contribution.exitStatus = ExitStatus.UNKNOWN
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("UNKNOWN") {
                                    end()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToEndWithStatus() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { contribution, _ ->
                        ++testStep1CallCount
                        contribution.exitStatus = ExitStatus.UNKNOWN
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("UNKNOWN") {
                                    end("TEST")
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus("TEST").exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
        }

        @Test
        fun testTransitionToFail() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var testStep1CallCount = 0
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        ++testStep1CallCount
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        flow("testFlow") {
                            step(testStep1) {
                                on("COMPLETED") {
                                    fail()
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.FAILED.exitCode)
            assertThat(testStep1CallCount).isEqualTo(1)
        }

        @Test
        fun testNoTransition() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val testStep1 = batch {
                step("testStep1") {
                    tasklet { _, _ ->
                        RepeatStatus.FINISHED
                    }
                }
            }

            // when, then
            assertThatThrownBy {
                batch {
                    job("testJob") {
                        flows {
                            flow("testFlow") {
                                step(testStep1) {
                                    on("COMPLETED") {
                                    }
                                }
                            }
                        }
                    }
                }
            }.hasMessageContaining("should set transition")
        }
    }

    @Nested
    inner class SplitBuilderDslTests {

        @Test
        fun testFlowBean() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val callerThread = Thread.currentThread().name
            var taskExecutorCallCount = 0
            val taskExecutor = object : ThreadPoolTaskExecutor() {
                override fun execute(task: Runnable) {
                    ++taskExecutorCallCount
                    super.execute(task)
                }
            }.apply { initialize() }
            var testStep1CallCount = 0
            var testStep2CallCount = 0
            val testFlow1 = batch {
                flow("testFlow1") {
                    step("testStep1") {
                        tasklet { _, _ ->
                            ++testStep1CallCount
                            assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            val testFlow2 = batch {
                flow("testFlow2") {
                    step("testStep2") {
                        tasklet { _, _ ->
                            ++testStep2CallCount
                            assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            context.apply {
                registerBean("testFlow1", Flow::class.java, Supplier { testFlow1 })
                registerBean("testFlow2", Flow::class.java, Supplier { testFlow2 })
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        split(taskExecutor) {
                            flowBean("testFlow1")
                            flowBean("testFlow2")
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(taskExecutorCallCount).isEqualTo(2)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testStep2CallCount).isEqualTo(1)
        }

        @Test
        fun testFlowWithInit() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            var taskExecutorCallCount = 0
            var testStep1CallCount = 0
            var testStep2CallCount = 0
            val callerThread = Thread.currentThread().name
            val taskExecutor = object : ThreadPoolTaskExecutor() {
                override fun execute(task: Runnable) {
                    ++taskExecutorCallCount
                    super.execute(task)
                }
            }.apply { initialize() }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        split(taskExecutor) {
                            flow("testFlow1") {
                                step("testStep1") {
                                    tasklet { _, _ ->
                                        ++testStep1CallCount
                                        assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                            flow("testFlow2") {
                                step("testStep2") {
                                    tasklet { _, _ ->
                                        ++testStep2CallCount
                                        assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                                        RepeatStatus.FINISHED
                                    }
                                }
                            }
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(taskExecutorCallCount).isEqualTo(2)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testStep2CallCount).isEqualTo(1)
        }

        @Test
        fun testFlowByVariable() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val jobLauncher = context.getBean(JobLauncher::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val callerThread = Thread.currentThread().name
            var taskExecutorCallCount = 0
            val taskExecutor = object : ThreadPoolTaskExecutor() {
                override fun execute(task: Runnable) {
                    ++taskExecutorCallCount
                    super.execute(task)
                }
            }.apply { initialize() }
            var testStep1CallCount = 0
            var testStep2CallCount = 0
            val testFlow1 = batch {
                flow("testFlow1") {
                    step("testStep1") {
                        tasklet { _, _ ->
                            ++testStep1CallCount
                            assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
            val testFlow2 = batch {
                flow("testFlow2") {
                    step("testStep2") {
                        tasklet { _, _ ->
                            ++testStep2CallCount
                            assertThat(Thread.currentThread().name).isNotEqualTo(callerThread)
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }

            // when
            val job = batch {
                job("testJob") {
                    flows {
                        split(taskExecutor) {
                            flow(testFlow1)
                            flow(testFlow2)
                        }
                    }
                }
            }
            val jobExecution = jobLauncher.run(job, JobParameters())

            // then
            assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(jobExecution.exitStatus.exitCode).isEqualTo(ExitStatus.COMPLETED.exitCode)
            assertThat(taskExecutorCallCount).isEqualTo(2)
            assertThat(testStep1CallCount).isEqualTo(1)
            assertThat(testStep2CallCount).isEqualTo(1)
        }

        @Test
        fun testNoFlow() {
            // given
            val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
            val batch = context.getBean(BatchDsl::class.java)
            val taskExecutor = object : ThreadPoolTaskExecutor() {
                override fun execute(task: Runnable) {
                    super.execute(task)
                }
            }.apply { initialize() }

            // when, then
            assertThatThrownBy {
                batch {
                    job("testJob") {
                        flows {
                            split(taskExecutor) {
                            }
                        }
                    }
                }
            }.hasMessageContaining("should set at least one flow to split")
        }
    }

    private fun randomExitStatus(): ExitStatus {
        return listOf(
            ExitStatus.UNKNOWN,
            ExitStatus.NOOP,
            ExitStatus.FAILED,
            ExitStatus.STOPPED,
            ExitStatus.COMPLETED,
            // ExitStatus.EXECUTING, // why considered ExitStatus.COMPLETE?
        ).random()
    }

    private fun randomFlowExecutionStatus(): FlowExecutionStatus {
        return listOf(
            FlowExecutionStatus.COMPLETED,
            FlowExecutionStatus.FAILED,
            FlowExecutionStatus.UNKNOWN,
            FlowExecutionStatus.STOPPED, // when stopped, just stop the job
        ).random()
    }

    @Configuration
    @EnableBatchProcessing
    @EnableBatchDsl
    private open class TestConfiguration {

        @Bean
        fun dataSource(): DataSource {
            return EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("/org/springframework/batch/core/schema-h2.sql")
                .generateUniqueName(true)
                .build()
        }
    }
}
