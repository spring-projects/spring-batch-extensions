/*
 * Copyright 2017-2021 the original author or authors.
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *          https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.batch.extensions.neo4j.builder;

import org.neo4j.driver.Driver;
import org.springframework.batch.extensions.neo4j.Neo4jItemWriter;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.util.Assert;

/**
 * A builder implementation for the {@link Neo4jItemWriter}
 *
 * @param <T> type of the entity to write
 *
 * @author Glenn Renfro
 * @author Gerrit Meier
 * @see Neo4jItemWriter
 */
public class Neo4jItemWriterBuilder<T> {

	private boolean delete = false;

	private Neo4jTemplate neo4jTemplate;
	private Driver neo4jDriver;
	private Neo4jMappingContext neo4jMappingContext;

	/**
	 * Boolean flag indicating whether the writer should save or delete the item at write
	 * time.
	 * @param delete true if write should delete item, false if item should be saved.
	 * Default is false.
	 * @return The current instance of the builder
	 * @see Neo4jItemWriter#setDelete(boolean)
	 */
	public Neo4jItemWriterBuilder<T> delete(boolean delete) {
		this.delete = delete;

		return this;
	}

	/**
	 * Establish the session factory that will be used to create {@link Neo4jTemplate} instances
	 * for interacting with Neo4j.
	 * @param neo4jTemplate neo4jTemplate to be used.
	 * @return The current instance of the builder
	 * @see Neo4jItemWriter#setNeo4jTemplate(Neo4jTemplate)
	 */
	public Neo4jItemWriterBuilder<T> neo4jTemplate(Neo4jTemplate neo4jTemplate) {
		this.neo4jTemplate = neo4jTemplate;

		return this;
	}

	/**
	 * Set the preconfigured Neo4j driver to be used within the built writer instance.
	 * @param neo4jDriver preconfigured Neo4j driver instance
	 * @return The current instance of the builder
	 */
	public Neo4jItemWriterBuilder<T> neo4jDriver(Driver neo4jDriver) {
		this.neo4jDriver = neo4jDriver;

		return this;
	}

	/**
	 * Set the Neo4jMappingContext to be used within the built writer instance.
	 * @param neo4jMappingContext initialized Neo4jMappingContext instance
	 * @return The current instance of the builder
	 */
	public Neo4jItemWriterBuilder<T> neo4jMappingContext(Neo4jMappingContext neo4jMappingContext) {
		this.neo4jMappingContext = neo4jMappingContext;

		return this;
	}

	/**
	 * Validates and builds a {@link org.springframework.batch.extensions.neo4j.Neo4jItemWriter}.
	 *
	 * @return a {@link Neo4jItemWriter}
	 */
	public Neo4jItemWriter<T> build() {
		Assert.notNull(neo4jTemplate, "neo4jTemplate is required.");
		Assert.notNull(neo4jDriver, "neo4jDriver is required.");
		Assert.notNull(neo4jMappingContext, "neo4jMappingContext is required.");
		Neo4jItemWriter<T> writer = new Neo4jItemWriter<>();
		writer.setDelete(this.delete);
		writer.setNeo4jTemplate(this.neo4jTemplate);
		writer.setNeo4jDriver(this.neo4jDriver);
		writer.setNeo4jMappingContext(this.neo4jMappingContext);

		return writer;
	}
}
