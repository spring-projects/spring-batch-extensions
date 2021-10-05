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
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.StepExecutionListener
import org.springframework.batch.core.annotation.AfterStep
import org.springframework.batch.core.annotation.BeforeStep
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.extensions.kotlindsl.configuration.BatchDsl
import org.springframework.batch.extensions.kotlindsl.configuration.EnableBatchDsl
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy
import org.springframework.batch.repeat.support.RepeatTemplate
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionStatus
import java.util.function.Supplier
import javax.sql.DataSource

/**
 * @author Taeik Lim
 */
internal class StepBuilderDslIntegrationTests {

    @Test
    fun testRepository() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val jobRepository = context.getBean(JobRepository::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var jobRepositoryCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        repository(
                            object : JobRepository by jobRepository {
                                override fun update(stepExecution: StepExecution) {
                                    ++jobRepositoryCallCount
                                    return jobRepository.update(stepExecution)
                                }
                            }
                        )
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobRepositoryCallCount).isGreaterThan(0)
    }

    @Test
    fun testTransactionManager() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val transactionManager = context.getBean(PlatformTransactionManager::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var transactionManagerCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        transactionManager(
                            object : PlatformTransactionManager by transactionManager {
                                override fun commit(status: TransactionStatus) {
                                    ++transactionManagerCallCount
                                    transactionManager.commit(status)
                                }
                            }
                        )
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(transactionManagerCallCount).isGreaterThan(0)
    }

    @Test
    fun testStartLimitAndAllowStartIfComplete() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        allowStartIfComplete(true)
                        startLimit(2)
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        val firstJobExecution = jobLauncher.run(job, JobParameters())
        val secondJobExecution = jobLauncher.run(job, JobParameters())
        val thirdJobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(firstJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(secondJobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(thirdJobExecution.status).isEqualTo(BatchStatus.FAILED)
    }

    @Test
    fun testObjectListener() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var beforeStepCallCount = 0
        var afterStepCallCount = 0

        @Suppress("unused")
        class TestListener {
            @BeforeStep
            fun beforeStep() {
                ++beforeStepCallCount
            }

            @AfterStep
            fun afterStep() {
                ++afterStepCallCount
            }
        }

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        listener(TestListener())
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeStepCallCount).isEqualTo(1)
        assertThat(afterStepCallCount).isEqualTo(1)
    }

    @Test
    fun testStepExecutionListener() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var beforeStepCallCount = 0
        var afterStepCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        listener(
                            object : StepExecutionListener {
                                override fun beforeStep(stepExecution: StepExecution) {
                                    ++beforeStepCallCount
                                }

                                override fun afterStep(stepExecution: StepExecution): ExitStatus? {
                                    ++afterStepCallCount
                                    return stepExecution.exitStatus
                                }
                            }
                        )
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeStepCallCount).isEqualTo(1)
        assertThat(afterStepCallCount).isEqualTo(1)
    }

    @Test
    fun testTaskletBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var taskletCallCount = 0
        val tasklet = Tasklet { _, _ ->
            ++taskletCallCount
            RepeatStatus.FINISHED
        }
        context.registerBean("testTasklet", Tasklet::class.java, Supplier { tasklet })

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        taskletBean("testTasklet")
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskletCallCount).isEqualTo(1)
    }

    @Test
    fun testTaskletBeanWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var taskletCallCount = 0
        var taskExecutorCallCount = 0
        val tasklet = Tasklet { _, _ ->
            ++taskletCallCount
            RepeatStatus.FINISHED
        }
        context.registerBean("testTasklet", Tasklet::class.java, Supplier { tasklet })

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        taskletBean("testTasklet") {
                            taskExecutor {
                                ++taskExecutorCallCount
                                it.run()
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskletCallCount).isEqualTo(1)
        assertThat(taskExecutorCallCount).isEqualTo(1)
    }

    @Test
    fun testTaskletByLambda() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var taskletCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        tasklet { _, _ ->
                            ++taskletCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskletCallCount).isEqualTo(1)
    }

    @Test
    fun testTaskletByLambdaWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var taskletCallCount = 0
        var taskExecutorCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        tasklet(
                            { _, _ ->
                                ++taskletCallCount
                                RepeatStatus.FINISHED
                            }
                        ) {
                            taskExecutor {
                                ++taskExecutorCallCount
                                it.run()
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(taskletCallCount).isEqualTo(1)
        assertThat(taskExecutorCallCount).isEqualTo(1)
    }

    @Test
    fun testChunkByCount() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        val readLimit = 20
        val chunkSize = 3
        var readCallCount = 0
        var writeCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        chunk<Int, Int>(chunkSize) {
                            reader {
                                if (readCallCount < readLimit) {
                                    ++readCallCount
                                    1
                                } else {
                                    null
                                }
                            }
                            writer {
                                ++writeCallCount
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(7) // Ceil(20/3)
    }

    @Test
    fun testChunkByCompletionPolicy() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        val readLimit = 20
        val chunkSize = 3
        var readCallCount = 0
        var writeCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        chunk<Int, Int>(SimpleCompletionPolicy(chunkSize)) {
                            reader {
                                if (readCallCount < readLimit) {
                                    ++readCallCount
                                    1
                                } else {
                                    null
                                }
                            }
                            writer {
                                ++writeCallCount
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(7) // Ceil(20/3)
    }

    @Test
    fun testChunkByRepeatOperations() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        val readLimit = 20
        val chunkSize = 3
        var readCallCount = 0
        var writeCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        chunk<Int, Int>(
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
                            writer {
                                ++writeCallCount
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(readCallCount).isEqualTo(readLimit)
        assertThat(writeCallCount).isEqualTo(7) // Ceil(20/3)
    }

    @Test
    fun testPartitioner() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var partitionHandlerCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        partitioner {
                            partitionHandler { _, _ ->
                                ++partitionHandlerCallCount
                                listOf()
                            }
                            splitter("splitStep") { mapOf() }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(partitionHandlerCallCount).isEqualTo(1)
    }

    @Test
    fun testJobBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var stepCallCount = 0
        val testJob2 = batch {
            job("testJob2") {
                steps {
                    step("testStep2") {
                        ++stepCallCount
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        context.registerBean("testJob2", Job::class.java, Supplier { testJob2 })

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        jobBean("testJob2")
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepCallCount).isEqualTo(1)
    }

    @Test
    fun testJobBeanWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var stepCallCount = 0
        var jobParametersExtractorCallCount = 0
        val testJob2 = batch {
            job("testJob2") {
                steps {
                    step("testStep2") {
                        ++stepCallCount
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        context.registerBean("testJob2", Job::class.java, Supplier { testJob2 })

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        jobBean("testJob2") {
                            parametersExtractor { _, _ ->
                                ++jobParametersExtractorCallCount
                                JobParameters()
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepCallCount).isEqualTo(1)
        assertThat(jobParametersExtractorCallCount).isEqualTo(1)
    }

    @Test
    fun testJobByJobVariable() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var stepCallCount = 0
        val testJob2 = batch {
            job("testJob2") {
                steps {
                    step("testStep2") {
                        ++stepCallCount
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        job(testJob2)
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepCallCount).isEqualTo(1)
    }

    @Test
    fun testJobByJobVariableWithInit() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var jobCallcount = 0
        var jobParametersExtractorCallCount = 0
        val testJob2 = batch {
            job("testJob2") {
                steps {
                    step("testStep2") {
                        ++jobCallcount
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        job(testJob2) {
                            parametersExtractor { _, _ ->
                                ++jobParametersExtractorCallCount
                                JobParameters()
                            }
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(jobCallcount).isEqualTo(1)
        assertThat(jobParametersExtractorCallCount).isEqualTo(1)
    }

    @Test
    fun testFlowBean() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0
        val testFlow = batch {
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
        context.registerBean("testFlow", Flow::class.java, Supplier { testFlow })

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        flowBean("testFlow")
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
    }

    @Test
    fun testFlowByLambda() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testStep1CallCount = 0
        var testStep2CallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
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
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
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
        val testFlow = batch {
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

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        flow(testFlow)
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(testStep1CallCount).isEqualTo(1)
        assertThat(testStep2CallCount).isEqualTo(1)
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
