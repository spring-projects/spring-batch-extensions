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

package org.springframework.batch.extensions.kotlindsl.flow

import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.batch.extensions.kotlindsl.step.StepBuilderDsl
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext
import org.springframework.core.task.TaskExecutor

/**
 * A dsl for [FlowBuilder][org.springframework.batch.core.job.builder.FlowBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
open class FlowBuilderDsl<T : Any> internal constructor(
    @Suppress("unused")
    private val dslContext: DslContext,
    private var flowBuilder: FlowBuilder<T>
) {
    private var started = false

    /**
     * Add step by bean name.
     */
    fun stepBean(name: String) {
        val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
        step(step)
    }

    /**
     * Add step.
     */
    fun step(name: String, stepInit: StepBuilderDsl.() -> Step) {
        val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
        val step = StepBuilderDsl(this.dslContext, stepBuilder).let(stepInit)
        step(step)
    }

    /**
     * Add step.
     */
    fun step(step: Step) {
        val baseFlowBuilder = if (!this.started) {
            this.started = true
            this.flowBuilder.start(step)
        } else {
            this.flowBuilder.next(step)
        }

        this.flowBuilder = baseFlowBuilder
    }

    /**
     * Add step by bean name with transition.
     */
    fun stepBean(name: String, stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit) {
        val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
        step(step, stepTransitionInit)
    }

    /**
     * Add step with transition.
     */
    fun step(
        name: String,
        stepInit: StepBuilderDsl.() -> Step,
        stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit
    ) {
        val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
        val step = StepBuilderDsl(this.dslContext, stepBuilder).let(stepInit)
        step(step, stepTransitionInit)
    }

    /**
     * Add step with transition.
     */
    fun step(step: Step, stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit) {
        val baseFlowBuilder = if (!this.started) {
            this.started = true
            this.flowBuilder.start(step)
        } else {
            this.flowBuilder.next(step)
        }

        this.flowBuilder = StepTransitionBuilderDsl<T>(this.dslContext, step, baseFlowBuilder)
            .apply(stepTransitionInit)
            .build()
    }

    /**
     * Add flow by bean name.
     */
    fun flowBean(name: String) {
        val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
        flow(flow)
    }

    /**
     * Add flow.
     */
    fun flow(name: String, flowInit: FlowBuilderDsl<Flow>.() -> Unit) {
        val flowBuilder = FlowBuilder<Flow>(name)
        val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
        flow(flow)
    }

    /**
     * Add flow.
     */
    fun flow(flow: Flow) {
        val baseFlowBuilder = if (!this.started) {
            this.started = true
            this.flowBuilder.start(flow)
        } else {
            this.flowBuilder.next(flow)
        }

        this.flowBuilder = baseFlowBuilder
    }

    /**
     * Add flow by bean name with transition.
     */
    fun flowBean(name: String, flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit) {
        val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
        flow(flow, flowTransitionInit)
    }

    /**
     * Add flow with transition.
     */
    fun flow(
        name: String,
        flowInit: FlowBuilderDsl<Flow>.() -> Unit,
        flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit
    ) {
        val flowBuilder = FlowBuilder<Flow>(name)
        val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
        flow(flow, flowTransitionInit)
    }

    /**
     * Add flow with transition.
     */
    fun flow(flow: Flow, flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit) {
        val baseFlowBuilder = if (!this.started) {
            this.started = true
            this.flowBuilder.start(flow)
        } else {
            this.flowBuilder.next(flow)
        }

        this.flowBuilder = FlowTransitionBuilderDsl<T>(this.dslContext, flow, baseFlowBuilder)
            .apply(flowTransitionInit)
            .build()
    }

    /**
     * Add decider by bean name with transition.
     */
    fun deciderBean(
        name: String,
        deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
    ) {
        val decider = this.dslContext.beanFactory.getBean(name, JobExecutionDecider::class.java)
        decider(decider, deciderTransitionInit)
    }

    /**
     * Add decider with transition.
     */
    fun decider(
        decider: JobExecutionDecider,
        deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
    ) {
        val baseUnterminatedFlowBuilder = if (!started) {
            this.flowBuilder.start(decider)
        } else {
            this.flowBuilder.next(decider)
        }

        this.flowBuilder = DeciderTransitionBuilderDsl<T>(this.dslContext, decider, baseUnterminatedFlowBuilder)
            .apply(deciderTransitionInit)
            .build()
    }

    /**
     * Split flow.
     *
     * @see [FlowBuilder.split][org.springframework.batch.core.job.builder.FlowBuilder.split]
     */
    fun split(taskExecutor: TaskExecutor, splitInit: SplitBuilderDsl<T>.() -> Unit) {
        val splitBuilder = this.flowBuilder.split(taskExecutor)
        this.flowBuilder = SplitBuilderDsl<T>(this.dslContext, splitBuilder).apply(splitInit).build()
    }

    internal fun build(): T = this.flowBuilder.build()

    /**
     * A dsl for step transition.
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @Suppress("DuplicatedCode")
    @BatchDslMarker
    class StepTransitionBuilderDsl<T : Any> internal constructor(
        private val dslContext: DslContext,
        private val step: Step,
        private val baseFlowBuilder: FlowBuilder<T>
    ) {
        private var flowBuilder: FlowBuilder<T>? = null

        /**
         * Set transition for state.
         *
         * @see [org.springframework.batch.core.job.builder.FlowBuilder.on]
         */
        fun on(pattern: String, init: TransitionBuilderDsl<T>.() -> Unit) {
            val flowBuilder = this.flowBuilder

            val transitionBuilder = if (flowBuilder == null) {
                this.baseFlowBuilder.on(pattern)
            } else {
                flowBuilder.from(this.step).on(pattern)
            }

            this.flowBuilder = TransitionBuilderDsl(this.dslContext, transitionBuilder).apply(init).build()
        }

        internal fun build(): FlowBuilder<T> {
            return checkNotNull(this.flowBuilder) {
                "should set transition for step ${step.name}."
            }
        }
    }

    /**
     * A dsl for flow transition.
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @Suppress("DuplicatedCode")
    @BatchDslMarker
    class FlowTransitionBuilderDsl<T : Any> internal constructor(
        private val dslContext: DslContext,
        private val flow: Flow,
        private val baseFlowBuilder: FlowBuilder<T>
    ) {
        private var flowBuilder: FlowBuilder<T>? = null

        /**
         * Set transition for state.
         *
         * @see [org.springframework.batch.core.job.builder.FlowBuilder.on]
         */
        fun on(pattern: String, init: TransitionBuilderDsl<T>.() -> Unit) {
            val flowBuilder = this.flowBuilder

            val transitionBuilder = if (flowBuilder == null) {
                this.baseFlowBuilder.on(pattern)
            } else {
                flowBuilder.from(this.flow).on(pattern)
            }

            this.flowBuilder = TransitionBuilderDsl(this.dslContext, transitionBuilder).apply(init).build()
        }

        internal fun build(): FlowBuilder<T> {
            return checkNotNull(this.flowBuilder) {
                "should set transition for flow ${flow.name}"
            }
        }
    }

    /**
     * A dsl for decider transition.
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @Suppress("DuplicatedCode")
    @BatchDslMarker
    class DeciderTransitionBuilderDsl<T : Any> internal constructor(
        private val dslContext: DslContext,
        private val decider: JobExecutionDecider,
        private val baseUnterminatedFlowBuilder: FlowBuilder.UnterminatedFlowBuilder<T>
    ) {
        private var flowBuilder: FlowBuilder<T>? = null

        /**
         * Set transition for state.
         *
         * @see [org.springframework.batch.core.job.builder.FlowBuilder.on]
         */
        fun on(pattern: String, init: TransitionBuilderDsl<T>.() -> Unit) {
            val flowBuilder = this.flowBuilder

            val transitionBuilder = if (flowBuilder == null) {
                this.baseUnterminatedFlowBuilder.on(pattern)
            } else {
                flowBuilder.from(this.decider).on(pattern)
            }

            this.flowBuilder = TransitionBuilderDsl(this.dslContext, transitionBuilder).apply(init).build()
        }

        internal fun build(): FlowBuilder<T> {
            return checkNotNull(this.flowBuilder) {
                "should set transition for decider $decider."
            }
        }
    }

    /**
     * A dsl for [FlowBuilder.TransitionBuilder][org.springframework.batch.core.job.builder.FlowBuilder.TransitionBuilder].
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @BatchDslMarker
    class TransitionBuilderDsl<T : Any> internal constructor(
        private val dslContext: DslContext,
        private val baseTransitionBuilder: FlowBuilder.TransitionBuilder<T>
    ) {
        private var flowBuilder: FlowBuilder<T>? = null

        /**
         * Transition to step by bean name.
         */
        fun stepBean(name: String) {
            val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
            step(step)
        }

        /**
         * Transition to step.
         */
        fun step(name: String, stepInit: StepBuilderDsl.() -> Step) {
            val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
            val step = StepBuilderDsl(this.dslContext, stepBuilder).let(stepInit)
            step(step)
        }

        /**
         * Transition to step.
         */
        fun step(step: Step) {
            this.flowBuilder = this.baseTransitionBuilder.to(step).from(step)
        }

        /**
         * Transition to step by bean name and set another transition.
         */
        fun stepBean(name: String, stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit) {
            val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
            step(step, stepTransitionInit)
        }

        /**
         * Transition to step and set another transition.
         */
        fun step(
            name: String,
            stepInit: StepBuilderDsl.() -> Step,
            stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit
        ) {
            val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
            val step = StepBuilderDsl(this.dslContext, stepBuilder).let(stepInit)
            step(step, stepTransitionInit)
        }

        /**
         * Transition to step and set another transition.
         */
        fun step(step: Step, stepTransitionInit: StepTransitionBuilderDsl<T>.() -> Unit) {
            val baseFlowBuilder = this.baseTransitionBuilder.to(step).from(step)
            this.flowBuilder = StepTransitionBuilderDsl<T>(this.dslContext, step, baseFlowBuilder)
                .apply(stepTransitionInit)
                .build()
        }

        /**
         * Transition to flow by bean name.
         */
        fun flowBean(name: String) {
            val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
            flow(flow)
        }

        /**
         * Transition to flow.
         */
        fun flow(name: String, flowInit: FlowBuilderDsl<Flow>.() -> Unit) {
            val flowBuilder = FlowBuilder<Flow>(name)
            val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
            flow(flow)
        }

        /**
         * Transition to flow.
         */
        fun flow(flow: Flow) {
            this.flowBuilder = this.baseTransitionBuilder.to(flow).from(flow)
        }

        /**
         * Transition to flow by bean name and set another transition.
         */
        fun flowBean(name: String, flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit) {
            val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
            flow(flow, flowTransitionInit)
        }

        /**
         * Transition to flow and set another transition.
         */
        fun flow(
            name: String,
            flowInit: FlowBuilderDsl<Flow>.() -> Unit,
            flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit
        ) {
            val flowBuilder = FlowBuilder<Flow>(name)
            val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
            flow(flow, flowTransitionInit)
        }

        /**
         * Transition to flow and set another transition.
         */
        fun flow(flow: Flow, flowTransitionInit: FlowTransitionBuilderDsl<T>.() -> Unit) {
            val baseFlowBuilder = this.baseTransitionBuilder.to(flow).from(flow)
            this.flowBuilder = FlowTransitionBuilderDsl<T>(this.dslContext, flow, baseFlowBuilder)
                .apply(flowTransitionInit)
                .build()
        }

        /**
         * Transition to decider by bean name and set another transition.
         */
        fun deciderBean(
            name: String,
            deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
        ) {
            val decider = this.dslContext.beanFactory.getBean(name, JobExecutionDecider::class.java)
            decider(decider, deciderTransitionInit)
        }

        /**
         * Transition to decider and set another transition.
         */
        fun decider(
            decider: JobExecutionDecider,
            deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
        ) {
            val baseUnterminatedFlowBuilder = this.baseTransitionBuilder.to(decider).from(decider)
            this.flowBuilder = DeciderTransitionBuilderDsl<T>(this.dslContext, decider, baseUnterminatedFlowBuilder)
                .apply(deciderTransitionInit)
                .build()
        }

        /**
         * Transition to stop.
         */
        fun stop() {
            this.flowBuilder = this.baseTransitionBuilder.stop()
        }

        /**
         * Transition to stop and restart with flow by bean name if the flow is restarted.
         */
        fun stopAndRestartToFlowBean(name: String) {
            val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
            stopAndRestartToFlow(flow)
        }

        /**
         * Transition to stop and restart with flow if the flow is restarted.
         */
        fun stopAndRestartToFlow(name: String, flowInit: FlowBuilderDsl<Flow>.() -> Unit) {
            val flowBuilder = FlowBuilder<Flow>(name)
            val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
            stopAndRestartToFlow(flow)
        }

        /**
         * Transition to stop and restart with flow if the flow is restarted.
         */
        fun stopAndRestartToFlow(flow: Flow) {
            this.flowBuilder = this.baseTransitionBuilder.stopAndRestart(flow)
        }

        /**
         * Transition to stop and restart with decider by bean name if the flow is restarted.
         */
        fun stopAndRestartToDeciderBean(
            name: String,
            deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
        ) {
            val decider = this.dslContext.beanFactory.getBean(name, JobExecutionDecider::class.java)
            stopAndRestartToDecider(decider, deciderTransitionInit)
        }

        /**
         * Transition to stop and restart with decider if the flow is restarted.
         */
        fun stopAndRestartToDecider(
            decider: JobExecutionDecider,
            deciderTransitionInit: DeciderTransitionBuilderDsl<T>.() -> Unit
        ) {
            val baseFlowBuilder = this.baseTransitionBuilder.stopAndRestart(decider).from(decider)
            this.flowBuilder = DeciderTransitionBuilderDsl(this.dslContext, decider, baseFlowBuilder)
                .apply(deciderTransitionInit)
                .build()
        }

        /**
         * Transition to stop and restart with step by bean name if the flow is restarted.
         */
        fun stopAndRestartToStepBean(name: String) {
            val step = this.dslContext.beanFactory.getBean(name, Step::class.java)
            stopAndRestartToStep(step)
        }

        /**
         * Transition to stop and restart with step if the flow is restarted.
         */
        fun stopAndRestartToStep(name: String, stepInit: StepBuilderDsl.() -> Step) {
            val stepBuilder = this.dslContext.stepBuilderFactory.get(name)
            val step = StepBuilderDsl(this.dslContext, stepBuilder).let(stepInit)
            stopAndRestartToStep(step)
        }

        /**
         * Transition to stop and restart with step if the flow is restarted.
         */
        fun stopAndRestartToStep(step: Step) {
            this.flowBuilder = this.baseTransitionBuilder.stopAndRestart(step)
        }

        /**
         * Transition to successful end.
         */
        fun end() {
            this.flowBuilder = this.baseTransitionBuilder.end()
        }

        /**
         * Transition to successful end with the status provided.
         */
        fun end(status: String) {
            this.flowBuilder = this.baseTransitionBuilder.end(status)
        }

        /**
         * Transition to fail.
         */
        fun fail() {
            this.flowBuilder = this.baseTransitionBuilder.fail()
        }

        internal fun build(): FlowBuilder<T> {
            return checkNotNull(this.flowBuilder) {
                "should set transition."
            }
        }
    }

    /**
     * A dsl for [FlowBuilderDsl.SplitBuilderDsl][org.springframework.batch.extensions.kotlindsl.flow.FlowBuilderDsl.SplitBuilderDsl].
     *
     * @author Taeik Lim
     * @since 0.1.0
     */
    @BatchDslMarker
    class SplitBuilderDsl<T : Any> internal constructor(
        private val dslContext: DslContext,
        private val splitBuilder: FlowBuilder.SplitBuilder<T>
    ) {
        private var flows = mutableListOf<Flow>()

        /**
         * Add flow to split by bean name.
         */
        fun flowBean(name: String) {
            val flow = this.dslContext.beanFactory.getBean(name, Flow::class.java)
            flow(flow)
        }

        /**
         * Add flow to split.
         */
        fun flow(name: String, flowInit: FlowBuilderDsl<Flow>.() -> Unit) {
            val flowBuilder = FlowBuilder<Flow>(name)
            val flow = FlowBuilderDsl(this.dslContext, flowBuilder).apply(flowInit).build()
            flow(flow)
        }

        /**
         * Add flow to split.
         */
        fun flow(flow: Flow) {
            flows.add(flow)
        }

        internal fun build(): FlowBuilder<T> {
            check(this.flows.isNotEmpty()) {
                "should set at least one flow to split."
            }

            return this.splitBuilder.add(*flows.toTypedArray())
        }
    }
}
