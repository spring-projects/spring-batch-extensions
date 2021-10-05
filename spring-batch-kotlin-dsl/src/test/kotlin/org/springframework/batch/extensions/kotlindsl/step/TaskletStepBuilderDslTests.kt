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
import org.springframework.batch.core.ChunkListener
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.annotation.AfterChunk
import org.springframework.batch.core.annotation.AfterChunkError
import org.springframework.batch.core.annotation.BeforeChunk
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.ItemStream
import org.springframework.batch.repeat.RepeatCallback
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.batch.repeat.support.RepeatTemplate
import org.springframework.batch.support.transaction.ResourcelessTransactionManager
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import org.springframework.transaction.support.TransactionSynchronizationManager

/**
 * @author Taeik Lim
 */
internal class TaskletStepBuilderDslTests {

    private val jobInstance = JobInstance(0L, "testJob")

    private val jobParameters = JobParameters()

    @Test
    fun testChunkListener() {
        // given
        var beforeChunkCallCount = 0
        var afterChunkCallCount = 0

        // when
        val step = taskletStepBuilderDsl(
            { _, _ -> RepeatStatus.FINISHED }
        ) {
            listener(
                object : ChunkListener {
                    override fun beforeChunk(context: ChunkContext) {
                        ++beforeChunkCallCount
                    }

                    override fun afterChunk(context: ChunkContext) {
                        ++afterChunkCallCount
                    }

                    override fun afterChunkError(context: ChunkContext) {
                        // no need to test. we are just testing if listener is invoked
                    }
                }
            )
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeChunkCallCount).isEqualTo(1)
        assertThat(afterChunkCallCount).isEqualTo(1)
    }

    @Test
    fun testObjectListener() {
        // given
        var beforeChunkCallCount = 0
        var afterChunkCallCount = 0

        // when
        class TestListener {
            @BeforeChunk
            fun beforeChunk() {
                ++beforeChunkCallCount
            }

            @AfterChunk
            fun afterChunk() {
                ++afterChunkCallCount
            }

            @AfterChunkError
            fun afterChunkError() {
                // no need to test. we are just testing if listener is invoked
            }
        }

        val step = taskletStepBuilderDsl(
            { _, _ -> RepeatStatus.FINISHED }
        ) {
            listener(TestListener())
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeChunkCallCount).isEqualTo(1)
        assertThat(afterChunkCallCount).isEqualTo(1)
    }

    @Test
    fun testStream() {
        // given
        var openCallCount = 0
        var updateCallCount = 0
        var closeCallCount = 0

        // when
        val step = taskletStepBuilderDsl(
            { _, _ -> RepeatStatus.FINISHED }
        ) {
            stream(
                object : ItemStream {
                    override fun open(executionContext: ExecutionContext) {
                        ++openCallCount
                    }

                    override fun update(executionContext: ExecutionContext) {
                        ++updateCallCount
                    }

                    override fun close() {
                        ++closeCallCount
                    }
                }
            )
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(openCallCount).isEqualTo(1)
        assertThat(updateCallCount).isGreaterThan(1)
        assertThat(closeCallCount).isEqualTo(1)
    }

    @Test
    fun testTaskExecutor() {
        // given
        var taskExecutorCallCount = 0

        // when
        val step = taskletStepBuilderDsl(
            { _, _ -> RepeatStatus.FINISHED }
        ) {
            taskExecutor { task ->
                ++taskExecutorCallCount
                task.run()
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskExecutorCallCount).isEqualTo(1)
    }

    @Test
    fun testExceptionHandler() {
        // given
        var exceptionHandlerCallCount = 0

        // when
        val step = taskletStepBuilderDsl(
            { _, _ -> throw RuntimeException("error") }
        ) {
            exceptionHandler { _, e ->
                ++exceptionHandlerCallCount
                throw e
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
        assertThat(exceptionHandlerCallCount).isEqualTo(1)
    }

    @Test
    fun testStepOperations() {
        // given
        var iterateCount = 0

        // when
        val step = taskletStepBuilderDsl(
            { _, _ -> RepeatStatus.FINISHED }
        ) {
            stepOperations(
                object : RepeatTemplate() {
                    override fun iterate(callback: RepeatCallback): RepeatStatus {
                        ++iterateCount
                        return super.iterate(callback)
                    }
                }
            )
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(iterateCount).isEqualTo(1)
    }

    @Test
    fun testStepOperationsAndRedundantSettings() {
        // given
        var iterateCount = 0
        var taskExecutorCallCount = 0
        var exceptionHandlerCallCount = 0
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }

        // when
        val step = stepBuilder
            .tasklet { _, _ -> RepeatStatus.FINISHED }
            .stepOperations(
                object : RepeatTemplate() {
                    override fun iterate(callback: RepeatCallback): RepeatStatus {
                        ++iterateCount
                        return super.iterate(callback)
                    }
                }
            )
            // redundant
            .taskExecutor { task ->
                ++taskExecutorCallCount
                task.run()
            }
            .exceptionHandler { _, e ->
                ++exceptionHandlerCallCount
                throw e
            }
            .build()
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(iterateCount).isEqualTo(1)
        assertThat(taskExecutorCallCount).isEqualTo(0)
        assertThat(exceptionHandlerCallCount).isEqualTo(0)
    }

    @Test
    fun testStepOperationsAndSetTaskExecutor() {
        assertThatThrownBy {
            taskletStepBuilderDsl(
                { _, _ -> throw RuntimeException("error") }
            ) {
                stepOperations(
                    object : RepeatTemplate() {
                        override fun iterate(callback: RepeatCallback): RepeatStatus {
                            return super.iterate(callback)
                        }
                    }
                )
                taskExecutor(SyncTaskExecutor())
            }
        }.hasMessageContaining("taskExecutor is redundant")
    }

    @Test
    fun testStepOperationsAndSetExceptionHandler() {
        assertThatThrownBy {
            taskletStepBuilderDsl(
                { _, _ -> throw RuntimeException("error") }
            ) {
                stepOperations(
                    object : RepeatTemplate() {
                        override fun iterate(callback: RepeatCallback): RepeatStatus {
                            return super.iterate(callback)
                        }
                    }
                )
                exceptionHandler { _, e ->
                    throw e
                }
            }
        }.hasMessageContaining("exceptionHandler is redundant")
    }

    @Test
    fun testTransactionalAttribute() {
        // when
        val step = taskletStepBuilderDsl(
            { _, _ ->
                val actual = TransactionSynchronizationManager.getCurrentTransactionName()
                assertThat(actual).isEqualTo("some_tx")

                RepeatStatus.FINISHED
            }
        ) {
            transactionAttribute(
                DefaultTransactionAttribute().apply {
                    name = "some_tx"
                }
            )
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
    }

    private fun taskletStepBuilderDsl(tasklet: Tasklet, init: TaskletStepBuilderDsl.() -> Unit): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val taskletStepBuilder = stepBuilder.tasklet(tasklet)

        return TaskletStepBuilderDsl(dslContext, taskletStepBuilder).apply(init).build()
    }
}
