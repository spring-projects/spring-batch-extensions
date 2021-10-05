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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.partition.PartitionHandler
import org.springframework.batch.core.partition.StepExecutionSplitter
import org.springframework.batch.core.partition.support.Partitioner
import org.springframework.batch.core.step.builder.PartitionStepBuilder
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.support.transaction.ResourcelessTransactionManager

/**
 * @author Taeik Lim
 */
internal class PartitionStepBuilderDslTests {

    private val jobInstance = JobInstance(0L, "testJob")

    private val jobParameters = JobParameters()

    @Test
    fun testPartitionHandlerAndDummySettings() {
        // given
        var partitionHandlerCallCount = 0
        var taskExecutorCallCount = 0
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val partitionStepBuilder = PartitionStepBuilder(stepBuilder)

        // when
        val step = partitionStepBuilder
            .partitionHandler { _, _ ->
                ++partitionHandlerCallCount
                listOf()
            }
            // dummy
            .step(
                object : Step {
                    override fun getName(): String {
                        throw RuntimeException("Should not be called")
                    }

                    override fun isAllowStartIfComplete(): Boolean {
                        throw RuntimeException("Should not be called")
                    }

                    override fun getStartLimit(): Int {
                        throw RuntimeException("Should not be called")
                    }

                    override fun execute(stepExecution: StepExecution) {
                        throw RuntimeException("Should not be called")
                    }
                }
            )
            .taskExecutor { task ->
                ++taskExecutorCallCount
                task.run()
            }
            .gridSize(3)
            .build()
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(partitionHandlerCallCount).isEqualTo(1)
        assertThat(taskExecutorCallCount).isEqualTo(0)
    }

    @Test
    fun testPartitionHandlerWithDirectOne() {
        // given
        var partitionHandlerCallCount = 0

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler { _, _ ->
                ++partitionHandlerCallCount
                listOf()
            }
            splitter("testStep") { mapOf() }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(partitionHandlerCallCount).isEqualTo(1)
    }

    @Test
    fun testPartitionHandlerWithTaskExecutorPartitionHandler() {
        // given
        var stepExecuteCallCount = 0
        var taskExecutorCallCount = 0
        val gridSize = 4

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler {
                step(
                    object : Step {
                        override fun getName(): String {
                            throw RuntimeException("Should not be called")
                        }

                        override fun isAllowStartIfComplete(): Boolean {
                            throw RuntimeException("Should not be called")
                        }

                        override fun getStartLimit(): Int {
                            throw RuntimeException("Should not be called")
                        }

                        override fun execute(stepExecution: StepExecution) {
                            ++stepExecuteCallCount
                            stepExecution.apply {
                                status = BatchStatus.COMPLETED
                                exitStatus = ExitStatus.COMPLETED
                            }
                        }
                    }
                )
                taskExecutor { task ->
                    ++taskExecutorCallCount
                    task.run()
                }
                gridSize(gridSize)
            }
            splitter("splitStep") { gridSize ->
                (0 until gridSize)
                    .associate { "hey$it" to ExecutionContext() }
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecuteCallCount).isEqualTo(gridSize)
        assertThat(taskExecutorCallCount).isEqualTo(gridSize)
        assertThat(jobExecution.stepExecutions).hasSize(gridSize + 1)
            .allSatisfy { assertThat(it.status).isEqualTo(BatchStatus.COMPLETED) }
            .anySatisfy { assertThat(it.stepName).contains("testStep") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey0") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey1") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey2") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey3") }
    }

    @Test
    fun testPartitionHandlerWithTaskExecutorPartitionHandlerWithoutTaskExecutor() {
        // given
        var stepExecuteCallCount = 0
        val gridSize = 4

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler {
                step(
                    object : Step {
                        override fun getName(): String {
                            throw RuntimeException("Should not be called")
                        }

                        override fun isAllowStartIfComplete(): Boolean {
                            throw RuntimeException("Should not be called")
                        }

                        override fun getStartLimit(): Int {
                            throw RuntimeException("Should not be called")
                        }

                        override fun execute(stepExecution: StepExecution) {
                            ++stepExecuteCallCount
                            stepExecution.apply {
                                status = BatchStatus.COMPLETED
                                exitStatus = ExitStatus.COMPLETED
                            }
                        }
                    }
                )
                gridSize(gridSize)
            }
            splitter("splitStep") { gridSize ->
                (0 until gridSize)
                    .associate { "hey$it" to ExecutionContext() }
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepExecuteCallCount).isEqualTo(gridSize)
        assertThat(jobExecution.stepExecutions).hasSize(gridSize + 1)
            .allSatisfy { assertThat(it.status).isEqualTo(BatchStatus.COMPLETED) }
            .anySatisfy { assertThat(it.stepName).contains("testStep") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey0") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey1") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey2") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey3") }
    }

    @Test
    fun testPartitionHandlerWithTaskExecutorPartitionHandlerWithoutGridSize() {
        // given
        var stepExecuteCallCount = 0
        var taskExecutorCallCount = 0
        val defaultGridSize = 6 // org.springframework.batch.core.step.builder.PartitionStepBuilder.DEFAULT_GRID_SIZE

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler {
                step(
                    object : Step {
                        override fun getName(): String {
                            throw RuntimeException("Should not be called")
                        }

                        override fun isAllowStartIfComplete(): Boolean {
                            throw RuntimeException("Should not be called")
                        }

                        override fun getStartLimit(): Int {
                            throw RuntimeException("Should not be called")
                        }

                        override fun execute(stepExecution: StepExecution) {
                            ++stepExecuteCallCount
                            stepExecution.apply {
                                status = BatchStatus.COMPLETED
                                exitStatus = ExitStatus.COMPLETED
                            }
                        }
                    }
                )
                taskExecutor { task ->
                    ++taskExecutorCallCount
                    task.run()
                }
            }
            splitter("splitStep") { gridSize ->
                (0 until gridSize)
                    .associate { "hey$it" to ExecutionContext() }
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecuteCallCount).isEqualTo(defaultGridSize)
        assertThat(taskExecutorCallCount).isEqualTo(defaultGridSize)
        assertThat(jobExecution.stepExecutions).hasSize(defaultGridSize + 1)
            .allSatisfy { assertThat(it.status).isEqualTo(BatchStatus.COMPLETED) }
            .anySatisfy { assertThat(it.stepName).contains("testStep") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey0") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey1") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey2") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey3") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey4") }
            .anySatisfy { assertThat(it.stepName).contains("splitStep:hey5") }
    }

    @Test
    fun testPartitionHandlerWithTaskExecutorPartitionHandlerWithoutStep() {
        assertThatThrownBy {
            partitionStepBuilderDsl {
                partitionHandler {
                    taskExecutor { task ->
                        task.run()
                    }
                    gridSize(3)
                }
                splitter("testStep") { gridSize ->
                    (0 until gridSize)
                        .associate { "hey$it" to ExecutionContext() }
                }
            }
        }.hasMessageContaining("step is not set")
    }

    @Suppress("ObjectLiteralToLambda")
    @Test
    fun testSplitterAndDummySettings() {
        // given
        var splitterCallCount = 0
        val dummyStepName = "dummyStepName"
        var partitionerCallCount = 0
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val partitionStepBuilder = PartitionStepBuilder(stepBuilder)

        // when
        val step = partitionStepBuilder
            .partitionHandler { stepSplitter, stepExecution ->
                stepSplitter.split(stepExecution, 1)
            }
            .splitter(
                object : StepExecutionSplitter {
                    override fun getStepName(): String {
                        return "testStep"
                    }

                    override fun split(stepExecution: StepExecution, gridSize: Int): Set<StepExecution> {
                        ++splitterCallCount
                        return setOf()
                    }
                }
            )
            // dummy
            .partitioner(
                dummyStepName,
                object : Partitioner {
                    override fun partition(gridSize: Int): Map<String, ExecutionContext> {
                        ++partitionerCallCount
                        return mapOf()
                    }
                }
            )
            .build()
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(splitterCallCount).isEqualTo(1)
        assertThat(stepExecution.stepName).isEqualTo("testStep")
        assertThat(partitionerCallCount).isEqualTo(0)
    }

    @Test
    fun testSplitterWithDirectOne() {
        // given
        var splitterCallCount = 0

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler { stepSplitter, stepExecution ->
                stepSplitter.split(stepExecution, 1)
            }
            splitter(
                object : StepExecutionSplitter {
                    override fun getStepName(): String {
                        return "splitStep"
                    }

                    override fun split(stepExecution: StepExecution, gridSize: Int): Set<StepExecution> {
                        ++splitterCallCount
                        return setOf()
                    }
                }
            )
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepExecution.stepName).isEqualTo("testStep")
        assertThat(splitterCallCount).isEqualTo(1)
    }

    @Test
    fun testSplitterWithSimpleStepExecutionSplitter() {
        // given
        var partitionerCallCount = 0

        // when
        val step = partitionStepBuilderDsl {
            partitionHandler { stepSplitter, stepExecution ->
                stepSplitter.split(stepExecution, 1)
            }
            splitter("splitStep") {
                ++partitionerCallCount
                mapOf()
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepExecution.stepName).isEqualTo("testStep")
        assertThat(partitionerCallCount).isEqualTo(1)
    }

    @Test
    fun testWithoutPartitionHandler() {
        assertThatThrownBy {
            partitionStepBuilderDsl {
                splitter("testStep") {
                    mapOf()
                }
            }
        }.hasMessageContaining("partitionHandler is not set")
    }

    @Test
    fun testWithoutSplitter() {
        assertThatThrownBy {
            partitionStepBuilderDsl {
                partitionHandler { stepSplitter, stepExecution ->
                    stepSplitter.split(stepExecution, 1)
                }
            }
        }.hasMessageContaining("splitter is not set")
    }

    @Test
    fun testAggregator() {
        // given
        var aggregatorCallCount = 0

        // when
        val step = partitionStepBuilderDsl {
            aggregator { _, _ ->
                ++aggregatorCallCount
            }
            partitionHandler(mock<PartitionHandler>())
            splitter(mock())
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(aggregatorCallCount).isEqualTo(1)
    }

    private fun partitionStepBuilderDsl(init: PartitionStepBuilderDsl.() -> Unit): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }

        return PartitionStepBuilderDsl(dslContext, PartitionStepBuilder(stepBuilder)).apply(init).build()
    }
}
