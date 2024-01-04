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

import org.neo4j.cypherdsl.core.StatementBuilder;
import org.springframework.batch.extensions.neo4j.Neo4jItemReader;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * A builder for the {@link Neo4jItemReader}.
 *
 * @param <T> type of the entity to read
 *
 * @author Glenn Renfro
 * @author Gerrit Meier
 * @see Neo4jItemReader
 */
public class Neo4jItemReaderBuilder<T> {

	private Neo4jTemplate neo4jTemplate;

	private StatementBuilder.OngoingReadingAndReturn statement;

	private Class<T> targetType;

	private Map<String, Object> parameterValues;

	private int pageSize = 10;

	private boolean saveState = true;

	private String name;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount;

	/**
	 * Configure if the state of the {@link org.springframework.batch.item.ItemStreamSupport}
	 * should be persisted within the {@link org.springframework.batch.item.ExecutionContext}
	 * for restart purposes.
	 *
	 * @param saveState defaults to true
	 * @return The current instance of the builder.
	 */
	public Neo4jItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;

		return this;
	}

	/**
	 * The name used to calculate the key within the
	 * {@link org.springframework.batch.item.ExecutionContext}. Required if
	 * {@link #saveState(boolean)} is set to true.
	 *
	 * @param name name of the reader instance
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.ItemStreamSupport#setName(String)
	 */
	public Neo4jItemReaderBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * Configure the max number of items to be read.
	 *
	 * @param maxItemCount the max items to be read
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setMaxItemCount(int)
	 */
	public Neo4jItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;

		return this;
	}

	/**
	 * Index for the current item. Used on restarts to indicate where to start from.
	 *
	 * @param currentItemCount current index
	 * @return this instance for method chaining
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setCurrentItemCount(int)
	 */
	public Neo4jItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;

		return this;
	}

	/**
	 * Establish the neo4jTemplate for the reader.
	 * @param neo4jTemplate the template to use for the reader.
	 * @return this instance for method chaining
	 * @see Neo4jItemReader#setNeo4jTemplate(Neo4jTemplate)
	 */
	public Neo4jItemReaderBuilder<T> neo4jTemplate(Neo4jTemplate neo4jTemplate) {
		this.neo4jTemplate = neo4jTemplate;

		return this;
	}

	/**
	 * The number of items to be read with each page.
	 *
	 * @param pageSize the number of items
	 * @return this instance for method chaining
	 * @see Neo4jItemReader#setPageSize(int)
	 */
	public Neo4jItemReaderBuilder<T> pageSize(int pageSize) {
		this.pageSize = pageSize;

		return this;
	}

	/**
	 * Optional parameters to be used in the cypher query.
	 *
	 * @param parameterValues the parameter values to be used in the cypher query
	 * @return this instance for method chaining
	 * @see Neo4jItemReader#setParameterValues(Map)
	 */
	public Neo4jItemReaderBuilder<T> parameterValues(Map<String, Object> parameterValues) {
		this.parameterValues = parameterValues;

		return this;
	}

	/**
	 * Cypher-DSL's {@link org.neo4j.cypherdsl.core.StatementBuilder.OngoingReadingAndReturn} statement
	 * without skip and limit segments. Those will get added by the pagination mechanism later.
	 *
	 * @param statement the cypher query without SKIP or LIMIT
	 * @return this instance for method chaining
	 * @see Neo4jItemReader#setStatement(org.neo4j.cypherdsl.core.StatementBuilder.OngoingReadingAndReturn)
	 */
	public Neo4jItemReaderBuilder<T> statement(StatementBuilder.OngoingReadingAndReturn statement) {
		this.statement = statement;

		return this;
	}

	/**
	 * The object type to be returned from each call to {@link Neo4jItemReader#read()}
	 *
	 * @param targetType the type of object to return.
	 * @return this instance for method chaining
	 * @see Neo4jItemReader#setTargetType(Class)
	 */
	public Neo4jItemReaderBuilder<T> targetType(Class<T> targetType) {
		this.targetType = targetType;

		return this;
	}

	/**
	 * Returns a fully constructed {@link Neo4jItemReader}.
	 *
	 * @return a new {@link Neo4jItemReader}
	 */
	public Neo4jItemReader<T> build() {
		if (this.saveState) {
			Assert.hasText(this.name, "A name is required when saveState is set to true");
		}
		Assert.notNull(this.neo4jTemplate, "neo4jTemplate is required.");
		Assert.notNull(this.targetType, "targetType is required.");
		Assert.notNull(this.statement, "statement is required.");
		Assert.isTrue(this.pageSize > 0, "pageSize must be greater than zero");
		Assert.isTrue(this.maxItemCount > 0, "maxItemCount must be greater than zero");
		Assert.isTrue(this.maxItemCount > this.currentItemCount , "maxItemCount must be greater than currentItemCount");

		Neo4jItemReader<T> reader = new Neo4jItemReader<>();
		reader.setPageSize(this.pageSize);
		reader.setParameterValues(this.parameterValues);
		reader.setNeo4jTemplate(this.neo4jTemplate);
		reader.setTargetType(this.targetType);
		reader.setStatement(this.statement);
		reader.setName(this.name);
		reader.setSaveState(this.saveState);
		reader.setCurrentItemCount(this.currentItemCount);
		reader.setMaxItemCount(this.maxItemCount);

		return reader;
	}

}
