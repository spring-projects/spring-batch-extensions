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

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.PartitionStepBuilder
import org.springframework.batch.core.step.builder.SimpleStepBuilder
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.extensions.kotlindsl.flow.FlowBuilderDsl
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.batch.repeat.CompletionPolicy
import org.springframework.batch.repeat.RepeatOperations
import org.springframework.transaction.PlatformTransactionManager

/**
 * A dsl for [StepBuilder][org.springframework.batch.core.step.builder.StepBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class StepBuilderDsl internal constructor(
    private val dslContext: DslContext,
    private val stepBuilder: StepBuilder
) {
    /**
     * Set for [StepBuilder.repository][org.springframework.batch.core.step.builder.StepBuilderHelper.repository].
     */
    fun repository(jobRepository: JobRepository) {
        this.stepBuilder.repository(jobRepository)
    }

    /**
     * Set for [StepBuilder.transactionManager][org.springframework.batch.core.step.builder.StepBuilderHelper.transactionManager].
     */
    fun transactionManager(transactionManager: PlatformTransactionManager) {
        this.stepBuilder.transactionManager(transactionManager)
    }

    /**
     * Set for [StepBuilder.startLimit][org.springframework.batch.core.step.builder.StepBuilderHelper.startLimit].
     */
    fun startLimit(startLimit: Int) {
        this.stepBuilder.startLimit(startLimit)
    }

    /**
     * Set listener processing followings.
     *
     * - [org.springframework.batch.core.annotation.BeforeStep]
     * - [org.springframework.batch.core.annotation.AfterStep]
     */
    fun listener(listener: Any) {
        this.stepBuilder.listener(listener)
    }

    /**
     * Set step execution listener.
     */
    fun listener(listener: StepExecutionListener) {
        this.stepBuilder.listener(listener)
    }

    /**
     * Set for [StepBuilder.allowStartIfComplete][org.springframework.batch.core.step.builder.StepBuilderHelper.allowStartIfComplete].
     */
    fun allowStartIfComplete(allowStartIfComplete: Boolean) {
        this.stepBuilder.allowStartIfComplete(allowStartIfComplete)
    }

    /**
     * Set tasklet step by bean name.
     */
    fun taskletBean(name: String): Step {
        return taskletBean(name) {}
    }

    /**
     * Set tasklet step by bean name.
     */
    fun taskletBean(name: String, taskletStepInit: TaskletStepBuilderDsl.() -> Unit): Step {
        val tasklet = this.dslContext.beanFactory.getBean(name, Tasklet::class.java)
        return tasklet(tasklet, taskletStepInit)
    }

    /**
     * Set tasklet step.
     */
    fun tasklet(tasklet: Tasklet): Step {
        return tasklet(tasklet) {}
    }

    /**
     * Set tasklet step.
     */
    fun tasklet(tasklet: Tasklet, taskletStepInit: TaskletStepBuilderDsl.() -> Unit): Step {
        val taskletStepBuilder = this.stepBuilder.tasklet(tasklet)
        return TaskletStepBuilderDsl(this.dslContext, taskletStepBuilder).apply(taskletStepInit).build()
    }

    /**
     * Set chunk-based step with a chunk size.
     */
    fun <I : Any?, O : Any?> chunk(chunkSize: Int, simpleStepInit: SimpleStepBuilderDsl<I, O>.() -> Unit): Step {
        val simpleStepBuilder = this.stepBuilder.chunk<I, O>(chunkSize)
        return SimpleStepBuilderDsl(this.dslContext, simpleStepBuilder).apply(simpleStepInit).build()
    }

    /**
     * Set chunk-based step with a completion policy.
     */
    fun <I : Any?, O : Any?> chunk(
        completionPolicy: CompletionPolicy,
        simpleStepInit: SimpleStepBuilderDsl<I, O>.() -> Unit
    ): Step {
        val simpleStepBuilder = this.stepBuilder.chunk<I, O>(completionPolicy)
        return SimpleStepBuilderDsl(this.dslContext, simpleStepBuilder).apply(simpleStepInit).build()
    }

    /**
     * Set chunk-based step with a repeat operations.
     */
    fun <I : Any?, O : Any?> chunk(
        repeatOperations: RepeatOperations,
        simpleStepInit: SimpleStepBuilderDsl<I, O>.() -> Unit
    ): Step {
        val simpleStepBuilder = SimpleStepBuilder<I, O>(this.stepBuilder).chunkOperations(repeatOperations)
        return SimpleStepBuilderDsl(this.dslContext, simpleStepBuilder).apply(simpleStepInit).build()
    }

    /**
     * Set partition step.
     */
    fun partitioner(partitionStepInit: PartitionStepBuilderDsl.() -> Unit): Step {
        val partitionStepBuilder = PartitionStepBuilder(this.stepBuilder)
        return PartitionStepBuilderDsl(this.dslContext, partitionStepBuilder).apply(partitionStepInit).build()
    }

    /**
     * Set job step by bean name.
     */
    fun jobBean(name: String): Step {
        return jobBean(name) {}
    }

    /**
     * Set job step by bean name.
     */
    fun jobBean(name: String, jobStepInit: JobStepBuilderDsl.() -> Unit): Step {
        val job = this.dslContext.beanFactory.getBean(name, Job::class.java)
        return job(job, jobStepInit)
    }

    /**
     * Set job step.
     */
    fun job(job: Job): Step {
        return job(job) {}
    }

    /**
     * Set job step.
     */
    fun job(job: Job, jobStepInit: JobStepBuilderDsl.() -> Unit): Step {
        val jobStepBuilder = this.stepBuilder.job(job)
        return JobStepBuilderDsl(this.dslContext, jobStepBuilder).apply(jobStepInit).build()
    }

    /**
     * Set flow step by bean name.
     */
    fun flowBean(name: String): Step {
        val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
        return flow(flow)
    }

    /**
     * Set flow step.
     */
    fun flow(name: String, flowInit: FlowBuilderDsl<Flow>.() -> Unit): Step {
        val flowBuilder = FlowBuilder<Flow>(name)
        val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
        return flow(flow)
    }

    /**
     * Set flow step.
     */
    fun flow(flow: Flow): Step {
        val flowStepBuilder = this.stepBuilder.flow(flow)
        return FlowStepBuilderDsl(this.dslContext, flowStepBuilder).build()
    }
}
