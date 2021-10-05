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

import org.springframework.batch.core.ChunkListener
import org.springframework.batch.core.ItemProcessListener
import org.springframework.batch.core.ItemReadListener
import org.springframework.batch.core.ItemWriteListener
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.Step
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder
import org.springframework.batch.core.step.builder.SimpleStepBuilder
import org.springframework.batch.core.step.item.KeyGenerator
import org.springframework.batch.core.step.skip.SkipPolicy
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemStream
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.repeat.RepeatOperations
import org.springframework.batch.repeat.exception.ExceptionHandler
import org.springframework.core.task.TaskExecutor
import org.springframework.retry.RetryListener
import org.springframework.retry.RetryPolicy
import org.springframework.retry.backoff.BackOffPolicy
import org.springframework.retry.policy.RetryContextCache
import org.springframework.transaction.interceptor.TransactionAttribute
import kotlin.reflect.KClass

/**
 * A dsl for [SimpleStepBuilder][org.springframework.batch.core.step.builder.SimpleStepBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class SimpleStepBuilderDsl<I : Any?, O : Any?> internal constructor(
    private val dslContext: DslContext,
    private var simpleStepBuilder: SimpleStepBuilder<I, O>
) {
    private var taskExecutorSet = false
    private var exceptionHandlerSet = false
    private var stepOperationsSet = false

    /**
     * Set faultTolerant config.
     *
     * @see [FaultTolerantStepBuilder][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder].
     */
    fun faultTolerant(init: FaultTolerantStepBuilderDsl<I, O>.() -> Unit) {
        val faultTolerantStepBuilder = this.simpleStepBuilder.faultTolerant()
        this.simpleStepBuilder = FaultTolerantStepBuilderDsl<I, O>(this.dslContext, faultTolerantStepBuilder)
            .apply(init)
            .build()
    }

    /**
     * Set for [SimpleStepBuilder.reader][org.springframework.batch.core.step.builder.SimpleStepBuilder.reader].
     */
    fun reader(reader: ItemReader<out I>) {
        this.simpleStepBuilder.reader(reader)
    }

    /**
     * Set for [SimpleStepBuilder.writer][org.springframework.batch.core.step.builder.SimpleStepBuilder.writer].
     */
    fun writer(writer: ItemWriter<in O>) {
        this.simpleStepBuilder.writer(writer)
    }

    /**
     * Set for [SimpleStepBuilder.processor][org.springframework.batch.core.step.builder.SimpleStepBuilder.processor].
     */
    fun processor(processor: ItemProcessor<in I, out O>) {
        this.simpleStepBuilder.processor(processor)
    }

    /**
     * Don't cache read item.
     *
     * @see [SimpleStepBuilder.readerIsTransactionalQueue][org.springframework.batch.core.step.builder.SimpleStepBuilder.readerIsTransactionalQueue].
     */
    fun readerIsTransactionalQueue() {
        this.simpleStepBuilder.readerIsTransactionalQueue()
    }

    /**
     * Set listener processing followings.
     *
     * - [org.springframework.batch.core.annotation.BeforeChunk]
     * - [org.springframework.batch.core.annotation.AfterChunk]
     * - [org.springframework.batch.core.annotation.AfterChunkError]
     * - [org.springframework.batch.core.annotation.BeforeRead]
     * - [org.springframework.batch.core.annotation.AfterRead]
     * - [org.springframework.batch.core.annotation.OnReadError]
     * - [org.springframework.batch.core.annotation.BeforeProcess]
     * - [org.springframework.batch.core.annotation.AfterProcess]
     * - [org.springframework.batch.core.annotation.OnProcessError]
     * - [org.springframework.batch.core.annotation.BeforeWrite]
     * - [org.springframework.batch.core.annotation.AfterWrite]
     * - [org.springframework.batch.core.annotation.OnWriteError]
     */
    fun listener(listener: Any) {
        this.simpleStepBuilder.listener(listener)
    }

    /**
     * Set item read listener.
     */
    fun listener(listener: ItemReadListener<in I>) {
        this.simpleStepBuilder.listener(listener)
    }

    /**
     * Set item write listener.
     */
    fun listener(listener: ItemWriteListener<in O>) {
        this.simpleStepBuilder.listener(listener)
    }

    /**
     * Set item process listener.
     */
    fun listener(listener: ItemProcessListener<in I, in O>) {
        this.simpleStepBuilder.listener(listener)
    }

    // from AbstractTaskletStepBuilder.xxx

    /**
     * Set for [SimpleStepBuilder.listener][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.listener].
     */
    fun listener(chunkListener: ChunkListener) {
        this.simpleStepBuilder.listener(chunkListener)
    }

    /**
     * Set for [SimpleStepBuilder.stream][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.stream].
     */
    fun stream(stream: ItemStream) {
        this.simpleStepBuilder.stream(stream)
    }

    /**
     * Set for [SimpleStepBuilder.taskExecutor][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.taskExecutor].
     * It can't be used when [stepOperations] is set.
     */
    fun taskExecutor(taskExecutor: TaskExecutor) {
        this.taskExecutorSet = true
        this.simpleStepBuilder.taskExecutor(taskExecutor)
    }

    /**
     * Set for [SimpleStepBuilder.throttleLimit][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.throttleLimit].
     * If not present, set as default value of [AbstractTaskletStepBuilder][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder].
     * It is redundant when no taskExecutor is set.
     */
    fun throttleLimit(throttleLimit: Int) {
        this.simpleStepBuilder.throttleLimit(throttleLimit)
    }

    /**
     * Set for [SimpleStepBuilder.exceptionHandler][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.exceptionHandler].
     * It can't be used when [stepOperations] is set.
     */
    fun exceptionHandler(exceptionHandler: ExceptionHandler) {
        this.exceptionHandlerSet = true
        this.simpleStepBuilder.exceptionHandler(exceptionHandler)
    }

    /**
     * Set for [SimpleStepBuilder.stepOperations][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.stepOperations].
     */
    fun stepOperations(repeatOperations: RepeatOperations) {
        this.stepOperationsSet = true
        this.simpleStepBuilder.stepOperations(repeatOperations)
    }

    /**
     * Set for [SimpleStepBuilder.transactionAttribute][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.transactionAttribute].
     */
    fun transactionAttribute(transactionAttribute: TransactionAttribute) {
        this.simpleStepBuilder.transactionAttribute(transactionAttribute)
    }

    internal fun build(): Step {
        // see org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.build
        if (this.stepOperationsSet) {
            check(!this.taskExecutorSet) {
                "taskExecutor is redundant when stepOperation is set."
            }
            check(!this.exceptionHandlerSet) {
                "exceptionHandler is redundant when stepOperation is set."
            }
        }

        return this.simpleStepBuilder.build()
    }

    /**
     * A dsl for [FaultTolerantStepBuilder][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder].
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @BatchDslMarker
    class FaultTolerantStepBuilderDsl<I : Any?, O : Any?> internal constructor(
        @Suppress("unused")
        private val dslContext: DslContext,
        private val faultTolerantStepBuilder: FaultTolerantStepBuilder<I, O>,
    ) {
        /**
         * Set for [FaultTolerantStepBuilder.retryLimit][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.retryLimit].
         */
        fun retryLimit(retryLimit: Int) {
            this.faultTolerantStepBuilder.retryLimit(retryLimit)
        }

        /**
         * Set for [FaultTolerantStepBuilder.noRetry][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.noRetry].
         */
        fun noRetry(type: KClass<out Throwable>) {
            this.faultTolerantStepBuilder.noRetry(type.java)
        }

        /**
         * Set for [FaultTolerantStepBuilder.retry][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.retry].
         */
        fun retry(type: KClass<out Throwable>) {
            this.faultTolerantStepBuilder.retry(type.java)
        }

        /**
         * Set for [FaultTolerantStepBuilder.retryPolicy][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.retryPolicy].
         */
        fun retryPolicy(retryPolicy: RetryPolicy) {
            this.faultTolerantStepBuilder.retryPolicy(retryPolicy)
        }

        /**
         * Set for [FaultTolerantStepBuilder.retryContextCache][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.retryContextCache].
         */
        fun retryContextCache(retryContextCache: RetryContextCache) {
            this.faultTolerantStepBuilder.retryContextCache(retryContextCache)
        }

        /**
         * Set retry listener.
         */
        fun listener(listener: RetryListener) {
            this.faultTolerantStepBuilder.listener(listener)
        }

        /**
         * Set for [FaultTolerantStepBuilder.keyGenerator][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.keyGenerator].
         */
        fun keyGenerator(keyGenerator: KeyGenerator) {
            this.faultTolerantStepBuilder.keyGenerator(keyGenerator)
        }

        /**
         * Set for [FaultTolerantStepBuilder.backOffPolicy][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.backOffPolicy].
         */
        fun backOffPolicy(backOffPolicy: BackOffPolicy) {
            this.faultTolerantStepBuilder.backOffPolicy(backOffPolicy)
        }

        /**
         * Set for [FaultTolerantStepBuilder.skipLimit][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.skipLimit].
         */
        fun skipLimit(skipLimit: Int) {
            this.faultTolerantStepBuilder.skipLimit(skipLimit)
        }

        /**
         * Set for [FaultTolerantStepBuilder.noSkip][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.noSkip].
         */
        fun noSkip(type: KClass<out Throwable>) {
            this.faultTolerantStepBuilder.noSkip(type.java)
        }

        /**
         * Set for [FaultTolerantStepBuilder.skip][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.skip].
         */
        fun skip(type: KClass<out Throwable>) {
            this.faultTolerantStepBuilder.skip(type.java)
        }

        /**
         * Set for [FaultTolerantStepBuilder.skipPolicy][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.skipPolicy].
         */
        fun skipPolicy(skipPolicy: SkipPolicy) {
            this.faultTolerantStepBuilder.skipPolicy(skipPolicy)
        }

        /**
         * Set listener processing followings.
         *
         * - [org.springframework.batch.core.annotation.OnSkipInRead]
         * - [org.springframework.batch.core.annotation.OnSkipInProcess]
         * - [org.springframework.batch.core.annotation.OnSkipInWrite]
         */
        fun listener(listener: Any) {
            this.faultTolerantStepBuilder.listener(listener)
        }

        /**
         * Set skip listener.
         */
        fun listener(listener: SkipListener<in I, in O>) {
            this.faultTolerantStepBuilder.listener(listener)
        }

        /**
         * Set for [FaultTolerantStepBuilder.noRollback][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.noRollback].
         */
        fun noRollback(type: KClass<out Throwable>) {
            this.faultTolerantStepBuilder.noRollback(type.java)
        }

        /**
         * Cache processor result between retries and during skip processing.
         *
         * @see [FaultTolerantStepBuilder.processorNonTransactional][org.springframework.batch.core.step.builder.FaultTolerantStepBuilder.processorNonTransactional].
         */
        fun processorNonTransactional() {
            this.faultTolerantStepBuilder.processorNonTransactional()
        }

        // FIXME: handle overridden one (except objectListener).
        /*
        fun listener(listener: ChunkListener) {
            this.faultTolerantStepBuilder.listener(listener)
        }

        fun listener(transactionAttribute: TransactionAttribute) {
            this.faultTolerantStepBuilder.transactionAttribute(transactionAttribute)
        }

        fun stream(stream: ItemStream) {
            this.faultTolerantStepBuilder.stream(stream)
        }
         */

        internal fun build(): SimpleStepBuilder<I, O> {
            return this.faultTolerantStepBuilder
        }
    }
}
