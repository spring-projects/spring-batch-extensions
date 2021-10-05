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

package org.springframework.batch.extensions.kotlindsl.job

import org.springframework.batch.core.job.builder.FlowJobBuilder
import org.springframework.batch.core.job.builder.JobFlowBuilder
import org.springframework.batch.extensions.kotlindsl.flow.FlowBuilderDsl
import org.springframework.batch.extensions.kotlindsl.support.BatchDslMarker
import org.springframework.batch.extensions.kotlindsl.support.DslContext

/**
 * A dsl for [FlowJobBuilder][org.springframework.batch.core.job.builder.FlowJobBuilder].
 *
 * @author Taeik Lim
 * @since 0.1.0
 */
@BatchDslMarker
class FlowJobBuilderDsl internal constructor(
    dslContext: DslContext,
    flowJobBuilder: FlowJobBuilder
) : FlowBuilderDsl<FlowJobBuilder>(dslContext, JobFlowBuilder(flowJobBuilder))
