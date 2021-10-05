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
import org.springframework.batch.core.Step
import org.springframework.batch.core.step.builder.TaskletStepBuilder
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.item.ItemStream
import org.springframework.batch.repeat.RepeatOperations
import org.springframework.batch.repeat.exception.ExceptionHandler
import org.springframework.core.task.TaskExecutor
import org.springframework.transaction.interceptor.TransactionAttribute

/**
 * A dsl for [TaskletStepBuilder][org.springframework.batch.core.step.builder.TaskletStepBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class TaskletStepBuilderDsl internal constructor(
    @Suppress("unused")
    private val dslContext: DslContext,
    private val taskletStepBuilder: TaskletStepBuilder
) {
    private var taskExecutorSet = false
    private var exceptionHandlerSet = false
    private var stepOperationsSet = false

    /**
     * Set for [TaskletStepBuilder.listener][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.listener].
     */
    fun listener(chunkListener: ChunkListener) {
        this.taskletStepBuilder.listener(chunkListener)
    }

    /**
     * Set listener processing followings.
     *
     * - [org.springframework.batch.core.annotation.BeforeChunk]
     * - [org.springframework.batch.core.annotation.AfterChunk]
     * - [org.springframework.batch.core.annotation.AfterChunkError]
     */
    fun listener(listener: Any) {
        this.taskletStepBuilder.listener(listener)
    }

    /**
     * Set for [TaskletStepBuilder.stream][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.stream].
     */
    fun stream(stream: ItemStream) {
        this.taskletStepBuilder.stream(stream)
    }

    /**
     * Set for [TaskletStepBuilder.taskExecutor][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.taskExecutor].
     * It can't be used when [stepOperations] is set.
     */
    fun taskExecutor(taskExecutor: TaskExecutor) {
        this.taskExecutorSet = true
        this.taskletStepBuilder.taskExecutor(taskExecutor)
    }

    // Maybe throttleLimit can be here. But throttleLimit is redundant in a tasklet step.

    /**
     * Set for [TaskletStepBuilder.exceptionHandler][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.exceptionHandler].
     * It can't be used when [stepOperations] is set.
     */
    fun exceptionHandler(exceptionHandler: ExceptionHandler) {
        this.exceptionHandlerSet = true
        this.taskletStepBuilder.exceptionHandler(exceptionHandler)
    }

    /**
     * Set for [TaskletStepBuilder.stepOperations][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.stepOperations].
     */
    fun stepOperations(repeatOperations: RepeatOperations) {
        this.stepOperationsSet = true
        this.taskletStepBuilder.stepOperations(repeatOperations)
    }

    /**
     * Set for [TaskletStepBuilder.transactionAttribute][org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder.transactionAttribute].
     */
    fun transactionAttribute(transactionAttribute: TransactionAttribute) {
        this.taskletStepBuilder.transactionAttribute(transactionAttribute)
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

        return this.taskletStepBuilder.build()
    }
}
