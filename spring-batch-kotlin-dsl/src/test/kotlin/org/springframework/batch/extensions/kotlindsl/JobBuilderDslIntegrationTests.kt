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
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.annotation.AfterJob
import org.springframework.batch.core.annotation.BeforeJob
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.explore.JobExplorer
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.extensions.kotlindsl.configuration.BatchDsl
import org.springframework.batch.extensions.kotlindsl.configuration.EnableBatchDsl
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType
import javax.sql.DataSource

/**
 * @author Taeik Lim
 */
internal class JobBuilderDslIntegrationTests {

    @Test
    fun testValidator() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var validatorCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                validator {
                    ++validatorCallCount
                }
                steps {
                    step("testStep") {
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(validatorCallCount).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun testIncrementer() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val jobExplorer = context.getBean(JobExplorer::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var incrementerCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                incrementer {
                    ++incrementerCallCount
                    it!!
                }
                steps {
                    step("testStep") {
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }
        val jobParameters = JobParametersBuilder(jobExplorer)
            .getNextJobParameters(job)
            .toJobParameters()
        val jobExecution = jobLauncher.run(job, jobParameters)

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(incrementerCallCount).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun testRepository() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val jobRepository = context.getBean(JobRepository::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var repositoryCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                repository(
                    object : JobRepository by jobRepository {
                        override fun update(jobExecution: JobExecution) {
                            ++repositoryCallCount
                            jobRepository.update(jobExecution)
                        }
                    }
                )
                steps {
                    step("testStep") {
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(repositoryCallCount).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun testObjectListener() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var beforeJobCallCount = 0
        var afterJobCallCount = 0

        @Suppress("unused")
        class TestListener {
            @BeforeJob
            fun beforeJob() {
                ++beforeJobCallCount
            }

            @AfterJob
            fun afterJob() {
                ++afterJobCallCount
            }
        }

        // when
        val job = batch {
            job("testJob") {
                listener(TestListener())
                steps {
                    step("testStep") {
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeJobCallCount).isEqualTo(1)
        assertThat(afterJobCallCount).isEqualTo(1)
    }

    @Test
    fun testJobExecutionListener() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var beforeJobCallCount = 0
        var afterJobCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                listener(
                    object : JobExecutionListener {
                        override fun beforeJob(jobExecution: JobExecution) {
                            ++beforeJobCallCount
                        }

                        override fun afterJob(jobExecution: JobExecution) {
                            ++afterJobCallCount
                        }
                    }
                )
                steps {
                    step("testStep") {
                        tasklet { _, _ -> RepeatStatus.FINISHED }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(beforeJobCallCount).isEqualTo(1)
        assertThat(afterJobCallCount).isEqualTo(1)
    }

    @Test
    fun testPreventRestart() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var tryCount = 0

        // when
        val job = batch {
            job("testJob") {
                preventRestart()
                steps {
                    step("testStep") {
                        tasklet { _, _ ->
                            if (tryCount == 0) {
                                ++tryCount
                                throw RuntimeException()
                            }
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
        }

        // then
        val jobExecution = jobLauncher.run(job, JobParameters())
        assertThat(jobExecution.status).isEqualTo(BatchStatus.FAILED)
        assertThatThrownBy {
            jobLauncher.run(job, JobParameters())
        }.hasMessageContaining("JobInstance already exists and is not restartable")
        assertThat(tryCount).isEqualTo(1)
    }

    @Test
    fun testSteps() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var testCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                steps {
                    step("testStep") {
                        tasklet { _, _ ->
                            ++testCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(testCallCount).isEqualTo(1)
    }

    @Test
    fun testFlows() {
        // given
        val context = AnnotationConfigApplicationContext(TestConfiguration::class.java)
        val jobLauncher = context.getBean(JobLauncher::class.java)
        val batch = context.getBean(BatchDsl::class.java)
        var stepCallCount = 0

        // when
        val job = batch {
            job("testJob") {
                flows {
                    step("testStep") {
                        tasklet { _, _ ->
                            ++stepCallCount
                            RepeatStatus.FINISHED
                        }
                    }
                }
            }
        }
        val jobExecution = jobLauncher.run(job, JobParameters())

        // then
        assertThat(jobExecution.status).isEqualTo(BatchStatus.COMPLETED)
        assertThat(stepCallCount).isEqualTo(1)
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
