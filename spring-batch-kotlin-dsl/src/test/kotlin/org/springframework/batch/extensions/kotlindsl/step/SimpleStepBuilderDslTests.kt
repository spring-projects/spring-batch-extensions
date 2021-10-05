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
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.ChunkListener
import org.springframework.batch.core.ItemProcessListener
import org.springframework.batch.core.ItemReadListener
import org.springframework.batch.core.ItemWriteListener
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobInstance
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.annotation.AfterChunk
import org.springframework.batch.core.annotation.AfterChunkError
import org.springframework.batch.core.annotation.AfterProcess
import org.springframework.batch.core.annotation.AfterRead
import org.springframework.batch.core.annotation.AfterWrite
import org.springframework.batch.core.annotation.BeforeChunk
import org.springframework.batch.core.annotation.BeforeProcess
import org.springframework.batch.core.annotation.BeforeRead
import org.springframework.batch.core.annotation.BeforeWrite
import org.springframework.batch.core.annotation.OnProcessError
import org.springframework.batch.core.annotation.OnReadError
import org.springframework.batch.core.annotation.OnSkipInRead
import org.springframework.batch.core.annotation.OnWriteError
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.builder.SimpleStepBuilder
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.item.ItemStream
import org.springframework.batch.repeat.CompletionPolicy
import org.springframework.batch.repeat.RepeatCallback
import org.springframework.batch.repeat.RepeatOperations
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy
import org.springframework.batch.repeat.support.RepeatTemplate
import org.springframework.batch.support.transaction.ResourcelessTransactionManager
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.retry.RetryCallback
import org.springframework.retry.RetryContext
import org.springframework.retry.RetryListener
import org.springframework.retry.backoff.FixedBackOffPolicy
import org.springframework.retry.policy.MapRetryContextCache
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.transaction.interceptor.DefaultTransactionAttribute
import org.springframework.transaction.support.TransactionSynchronizationManager
import kotlin.math.ceil

/**
 * @author Taeik Lim
 */
internal class SimpleStepBuilderDslTests {

    private val jobInstance = JobInstance(0L, "testJob")

    private val jobParameters = JobParameters()

    @Test
    fun testWithChunkSize() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var processCallCount = 0
        var writeCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor {
                ++processCallCount
                it
            }
            writer {
                ++writeCallCount
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(processCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testWithCompletionPolicy() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var processCallCount = 0
        var writeCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(SimpleCompletionPolicy(chunkSize)) {
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor {
                ++processCallCount
                it
            }
            writer {
                ++writeCallCount
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(processCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testWithRepeatOperations() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var processCallCount = 0
        var writeCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(
            RepeatTemplate().apply {
                setCompletionPolicy(SimpleCompletionPolicy(chunkSize))
            }
        ) {
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor {
                ++processCallCount
                it
            }
            writer {
                ++writeCallCount
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(processCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testReaderWriterProcessor() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var processCallCount = 0
        var writeCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor {
                ++processCallCount
                it
            }
            writer {
                ++writeCallCount
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(processCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testReaderIsTransactionalQueue() {
        // given
        val chunkSize = 5
        val readLimit = 3
        val retryLimit = 2
        var readCallCount = 0
        var readCounter = 0
        var tryCount = 1

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            reader {
                if (readCounter < readLimit) {
                    ++readCallCount
                    ++readCounter
                    1
                } else {
                    null
                }
            }
            writer {
                if (tryCount < retryLimit) {
                    readCounter = 0
                    ++tryCount
                    throw IllegalStateException("Error")
                }
            }
            readerIsTransactionalQueue()
            faultTolerant {
                retryLimit(retryLimit)
                retry(IllegalStateException::class)
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        // without readerIsTransactionalQueue, it would be same as readLimit since read result is cached.
        assertThat(readCallCount).isEqualTo(readLimit * tryCount)
    }

    @Test
    fun testObjectListener() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var beforeChunkCallCount = 0
        var afterChunkCallCount = 0
        var beforeReadCallCount = 0
        var afterReadCallCount = 0
        var beforeProcessCallCount = 0
        var afterProcessCallCount = 0
        var beforeWriteCallCount = 0
        var afterWriteCallCount = 0

        @Suppress("unused")
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

            @BeforeRead
            fun beforeRead() {
                ++beforeReadCallCount
            }

            @AfterRead
            fun afterRead() {
                ++afterReadCallCount
            }

            @OnReadError
            fun onReadError() {
                // no need to test. we are just testing if listener is invoked
            }

            @BeforeProcess
            fun beforeProcess() {
                ++beforeProcessCallCount
            }

            @AfterProcess
            fun afterProcess() {
                ++afterProcessCallCount
            }

            @OnProcessError
            fun onProcessError() {
                // no need to test. we are just testing if listener is invoked
            }

            @BeforeWrite
            fun beforeWrite() {
                ++beforeWriteCallCount
            }

            @AfterWrite
            fun afterWrite() {
                ++afterWriteCallCount
            }

            @OnWriteError
            fun onWriteError() {
                // no need to test. we are just testing if listener is invoked
            }
        }

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            listener(TestListener())
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor { it }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(beforeChunkCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
        assertThat(afterChunkCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
        assertThat(beforeReadCallCount).isEqualTo(readLimit + 1)
        assertThat(afterReadCallCount).isEqualTo(readLimit)
        assertThat(beforeProcessCallCount).isEqualTo(readLimit)
        assertThat(afterProcessCallCount).isEqualTo(readLimit)
        assertThat(beforeWriteCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
        assertThat(afterWriteCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testReadListener() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var beforeReadCallCount = 0
        var afterReadCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            listener(
                object : ItemReadListener<Int> {
                    override fun beforeRead() {
                        ++beforeReadCallCount
                    }

                    override fun afterRead(item: Int) {
                        ++afterReadCallCount
                    }

                    override fun onReadError(ex: Exception) {
                        // no need to test. we are just testing if listener is invoked
                    }
                }
            )
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(beforeReadCallCount).isEqualTo(readLimit + 1)
        assertThat(afterReadCallCount).isEqualTo(readLimit)
    }

    @Test
    fun testWriteListener() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var beforeWriteCallCount = 0
        var afterWriteCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            listener(
                object : ItemWriteListener<Int> {
                    override fun beforeWrite(items: MutableList<out Int>) {
                        ++beforeWriteCallCount
                    }

                    override fun afterWrite(items: MutableList<out Int>) {
                        ++afterWriteCallCount
                    }

                    override fun onWriteError(exception: Exception, items: MutableList<out Int>) {
                        // no need to test. we are just testing if listener is invoked
                    }
                }
            )
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(beforeWriteCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
        assertThat(afterWriteCallCount).isEqualTo(calculateWriteSize(readLimit, chunkSize))
    }

    @Test
    fun testProcessListener() {
        // given
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var beforeProcessCallCount = 0
        var afterProcessCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
            listener(
                object : ItemProcessListener<Int, Int> {
                    override fun beforeProcess(item: Int) {
                        ++beforeProcessCallCount
                    }

                    override fun afterProcess(item: Int, result: Int?) {
                        ++afterProcessCallCount
                    }

                    override fun onProcessError(item: Int, e: java.lang.Exception) {
                        // no need to test. we are just testing if listener is invoked
                    }
                }
            )
            reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor { it }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(beforeProcessCallCount).isEqualTo(readLimit)
        assertThat(afterProcessCallCount).isEqualTo(readLimit)
    }

    @Test
    fun testChunkListener() {
        // given
        var beforeChunkCallCount = 0
        var afterChunkCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(3) {
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
            reader { null }
            writer {}
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
        val step = simpleStepBuilderDsl<Int, Int>(1) {
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
            reader { null }
            writer {}
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
        val step = simpleStepBuilderDsl<Int, Int>(1) {
            taskExecutor { task ->
                ++taskExecutorCallCount
                task.run()
            }
            reader { null }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskExecutorCallCount).isEqualTo(1)
    }

    @Suppress("ForEachParameterNotUsed")
    @RepeatedTest(10)
    @Test
    fun testThrottleLimit() {
        // given
        var readCallCount = 0
        var processCallCount = 0
        val taskExecutor = ThreadPoolTaskExecutor().apply {
            corePoolSize = Runtime.getRuntime().availableProcessors()
            initialize()
        }

        // when
        val step = simpleStepBuilderDsl<Int, Int>(1) {
            taskExecutor(taskExecutor)
            throttleLimit(1)
            reader {
                if (readCallCount < 1000) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            processor {
                ++processCallCount
                it
            }
            writer {}
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(1000)
        assertThat(processCallCount).isEqualTo(1000)
    }

    @Test
    fun testExceptionHandler() {
        // given
        var exceptionHandlerCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(1) {
            exceptionHandler { _, e ->
                ++exceptionHandlerCallCount
                throw e
            }
            reader { throw RuntimeException("error") }
            writer {}
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
        val step = simpleStepBuilderDsl<Int, Int>(1) {
            stepOperations(
                object : RepeatTemplate() {
                    override fun iterate(callback: RepeatCallback): RepeatStatus {
                        ++iterateCount
                        return super.iterate(callback)
                    }
                }
            )
            reader { null }
            writer {}
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
        val chunkSize = 3
        val readLimit = 20
        var readCallCount = 0
        var iterateCount = 0
        var taskExecutorCallCount = 0
        var exceptionHandlerCallCount = 0
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }

        // when
        val step = stepBuilder
            .chunk<Int, Int>(chunkSize)
            .reader {
                if (readCallCount < readLimit) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            .writer { }
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
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(taskExecutorCallCount).isEqualTo(0)
        assertThat(exceptionHandlerCallCount).isEqualTo(0)
    }

    @Test
    fun testStepOperationsAndSetTaskExecutor() {
        assertThatThrownBy {
            simpleStepBuilderDsl<Int, Int>(1) {
                stepOperations(
                    object : RepeatTemplate() {
                        override fun iterate(callback: RepeatCallback): RepeatStatus {
                            return super.iterate(callback)
                        }
                    }
                )
                taskExecutor(SyncTaskExecutor())
                reader { null }
                writer {}
            }
        }.hasMessageContaining("taskExecutor is redundant")
    }

    @Test
    fun testStepOperationsAndSetExceptionHandler() {
        assertThatThrownBy {
            simpleStepBuilderDsl<Int, Int>(1) {
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
                reader { null }
                writer {}
            }
        }.hasMessageContaining("exceptionHandler is redundant")
    }

    @Test
    fun testTransactionalAttribute() {
        // given
        var readCallCount = 0

        // when
        val step = simpleStepBuilderDsl<Int, Int>(1) {
            transactionAttribute(
                DefaultTransactionAttribute().apply {
                    name = "some_tx"
                }
            )
            reader {
                if (readCallCount < 1) {
                    ++readCallCount
                    1
                } else {
                    null
                }
            }
            writer {
                val actual = TransactionSynchronizationManager.getCurrentTransactionName()
                assertThat(actual).isEqualTo("some_tx")
            }
        }
        val jobExecution = JobExecution(jobInstance, jobParameters)
        val stepExecution = jobExecution.createStepExecution(step.name)
        step.execute(stepExecution)

        // then
        assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(1)
    }

    @Nested
    inner class FaultTolerantStepBuilderDslTests {

        @Test
        fun testRetryLimit() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    ++tryCount
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retry(RuntimeException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(tryCount).isEqualTo(retryLimit)
        }

        @Test
        fun testRetryLimitWithNoRetry() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    ++tryCount
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retry(RuntimeException::class)
                    noRetry(IllegalStateException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(tryCount).isEqualTo(1)
        }

        @Test
        fun testRetryPolicy() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    ++tryCount
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryPolicy(SimpleRetryPolicy(retryLimit))
                    retry(RuntimeException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(tryCount).isEqualTo(retryLimit)
        }

        @Test
        fun testRetryContextCache() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var tryCount = 0
            var retryContextCacheCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    ++tryCount
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retryContextCache(
                        object : MapRetryContextCache() {
                            override fun containsKey(key: Any?): Boolean {
                                ++retryContextCacheCallCount
                                return super.containsKey(key)
                            }
                        }
                    )
                    retry(RuntimeException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(tryCount).isEqualTo(retryLimit)
            assertThat(retryContextCacheCallCount).isGreaterThan(0)
        }

        @Test
        fun testBackoffPolicy() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var backoffPolicyCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    backOffPolicy(
                        object : FixedBackOffPolicy() {
                            override fun doBackOff() {
                                ++backoffPolicyCallCount
                                super.doBackOff()
                            }
                        }
                    )
                    retry(RuntimeException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(backoffPolicyCallCount).isGreaterThan(0)
        }

        @Test
        fun testRetryListener() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var retryOpenCallCount = 0
            var retryCloseCallCount = 0
            var retryOnErrorCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retry(IllegalStateException::class)
                    listener(
                        object : RetryListener {
                            override fun <T : Any?, E : Throwable?> open(
                                context: RetryContext?,
                                callback: RetryCallback<T, E>?
                            ): Boolean {
                                ++retryOpenCallCount
                                return true
                            }

                            override fun <T : Any?, E : Throwable?> close(
                                context: RetryContext?,
                                callback: RetryCallback<T, E>?,
                                throwable: Throwable?
                            ) {
                                ++retryCloseCallCount
                            }

                            override fun <T : Any?, E : Throwable?> onError(
                                context: RetryContext?,
                                callback: RetryCallback<T, E>?,
                                throwable: Throwable?
                            ) {
                                ++retryOnErrorCallCount
                            }
                        }
                    )
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(retryOpenCallCount).isGreaterThan(0)
            assertThat(retryCloseCallCount).isGreaterThan(0)
            assertThat(retryOnErrorCallCount).isGreaterThan(0)
        }

        @Test
        fun testKeyGenerator() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var keyGeneratorCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retry(IllegalStateException::class)
                    keyGenerator {
                        ++keyGeneratorCallCount
                        "testkey"
                    }
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(readCallCount).isEqualTo(chunkSize)
            assertThat(keyGeneratorCallCount).isGreaterThan(0)
        }

        @Test
        fun testSkipLimit() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val skipLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (tryCount < skipLimit) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }

                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer { }
                faultTolerant {
                    skipLimit(skipLimit)
                    skip(IllegalStateException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
            assertThat(tryCount).isEqualTo(skipLimit)
        }

        @Test
        fun testSkipLimitWithNoSkip() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val skipLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (tryCount < skipLimit) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }

                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer { }
                faultTolerant {
                    skipLimit(skipLimit)
                    skip(RuntimeException::class)
                    noSkip(IllegalStateException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.FAILED)
            assertThat(tryCount).isEqualTo(1)
            assertThat(readCallCount).isEqualTo(0)
        }

        @Test
        fun testSkipPolicy() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val skipLimit = 3
            var readCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (tryCount < skipLimit) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }

                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer { }
                faultTolerant {
                    skipPolicy(LimitCheckingItemSkipPolicy(skipLimit, mapOf(IllegalStateException::class.java to true)))
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
            assertThat(tryCount).isEqualTo(skipLimit)
        }

        @Test
        fun testObjectSkipListener() {
            // given
            val chunkSize = 1
            val readLimit = 3
            val skipLimit = 1
            val tryLimit = 1
            var readCallCount = 0
            var tryCount = 0
            var onSkipInReadCallCount = 0

            class TestListener {
                @Suppress("unused")
                @OnSkipInRead
                fun onSkipInRead() {
                    ++onSkipInReadCallCount
                }

                // test bindings only
            }

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (tryCount < tryLimit) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }

                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {}
                faultTolerant {
                    skipLimit(skipLimit)
                    skip(IllegalStateException::class)
                    listener(TestListener())
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
            assertThat(tryCount).isEqualTo(tryLimit)
            assertThat(onSkipInReadCallCount).isEqualTo(tryLimit)
        }

        @Test
        fun testSkipListener() {
            // given
            val chunkSize = 1
            val readLimit = 3
            val skipLimit = 1
            val tryLimit = 1
            var readCallCount = 0
            var tryCount = 0
            var onSkipInReadCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (tryCount < tryLimit) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }

                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {}
                readerIsTransactionalQueue()
                faultTolerant {
                    skipLimit(skipLimit)
                    skip(IllegalStateException::class)
                    listener(
                        object : SkipListener<Int, Int> {
                            override fun onSkipInRead(t: Throwable) {
                                ++onSkipInReadCallCount
                            }

                            override fun onSkipInProcess(item: Int, t: Throwable) {
                                // test bindings only
                            }

                            override fun onSkipInWrite(item: Int, t: Throwable) {
                                // test bindings only
                            }
                        }
                    )
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
            assertThat(tryCount).isEqualTo(tryLimit)
            assertThat(onSkipInReadCallCount).isEqualTo(tryLimit)
        }

        @Test
        fun testNoRollback() {
            // given
            val chunkSize = 4
            val readLimit = 20
            var readCallCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                writer {
                    // ignored when noRollback is set
                    throw IllegalStateException("Error")
                }
                faultTolerant {
                    noRollback(IllegalStateException::class)
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
        }

        @Test
        fun testProcessorNonTransactional() {
            // given
            val chunkSize = 4
            val readLimit = 20
            val retryLimit = 3
            var readCallCount = 0
            var processCallCount = 0
            var tryCount = 0

            // when
            val step = simpleStepBuilderDsl<Int, Int>(chunkSize) {
                reader {
                    if (readCallCount < readLimit) {
                        ++readCallCount
                        1
                    } else {
                        null
                    }
                }
                processor {
                    ++processCallCount
                    it
                }
                writer {
                    if (tryCount < (retryLimit - 1)) {
                        ++tryCount
                        throw IllegalStateException("Error")
                    }
                }
                faultTolerant {
                    retryLimit(retryLimit)
                    retry(IllegalStateException::class)
                    processorNonTransactional()
                }
            }
            val jobExecution = JobExecution(jobInstance, jobParameters)
            val stepExecution = jobExecution.createStepExecution(step.name)
            step.execute(stepExecution)

            // then
            assertThat(stepExecution.status).isEqualTo(BatchStatus.COMPLETED)
            assertThat(readCallCount).isEqualTo(readLimit)
            // without processorNonTransactional, it would be 28 (readLimit + chunkSize * retryCount)
            assertThat(processCallCount).isEqualTo(readLimit)
        }
    }

    private fun <I : Any?, O : Any?> simpleStepBuilderDsl(
        chunkSize: Int,
        init: SimpleStepBuilderDsl<I, O>.() -> Unit
    ): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val simpleStepBuilder = stepBuilder.chunk<I, O>(chunkSize)

        return SimpleStepBuilderDsl(dslContext, simpleStepBuilder).apply(init).build()
    }

    private fun <I : Any?, O : Any?> simpleStepBuilderDsl(
        completionPolicy: CompletionPolicy,
        init: SimpleStepBuilderDsl<I, O>.() -> Unit
    ): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val simpleStepBuilder = stepBuilder.chunk<I, O>(completionPolicy)

        return SimpleStepBuilderDsl(dslContext, simpleStepBuilder).apply(init).build()
    }

    private fun <I : Any?, O : Any?> simpleStepBuilderDsl(
        repeatOperations: RepeatOperations,
        init: SimpleStepBuilderDsl<I, O>.() -> Unit
    ): Step {
        val dslContext = DslContext(
            beanFactory = mock(),
            jobBuilderFactory = mock(),
            stepBuilderFactory = mock(),
        )
        val stepBuilder = StepBuilder("testStep").apply {
            repository(mock())
            transactionManager(ResourcelessTransactionManager())
        }
        val simpleStepBuilder = SimpleStepBuilder<I, O>(stepBuilder).chunkOperations(repeatOperations)

        return SimpleStepBuilderDsl(dslContext, simpleStepBuilder).apply(init).build()
    }

    private fun calculateWriteSize(readLimit: Int, chunkSize: Int) =
        ceil(readLimit.toDouble() / chunkSize.toDouble()).toInt()
}
