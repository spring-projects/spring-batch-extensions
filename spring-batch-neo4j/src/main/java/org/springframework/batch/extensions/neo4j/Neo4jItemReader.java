/*
 * Copyright 2012-2021 the original author or authors.
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

package org.springframework.batch.extensions.neo4j;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.StatementBuilder;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.AbstractPaginatedDataItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.util.Assert;

import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * Restartable {@link ItemReader} that reads objects from the graph database Neo4j
 * via a paging technique.
 * </p>
 *
 * <p>
 * It executes cypher queries built from the statement provided to
 * retrieve the requested data.  The query is executed using paged requests of
 * a size specified in {@link #setPageSize(int)}.  Additional pages are requested
 * as needed when the {@link #read()} method is called.  On restart, the reader
 * will begin again at the same number item it left off at.
 * </p>
 *
 * <p>
 * Performance is dependent on your Neo4j configuration as
 * well as page size.  Setting a fairly large page size and using a commit
 * interval that matches the page size should provide better performance.
 * </p>
 *
 * <p>
 * This implementation is thread-safe between calls to
 * {@link #open(org.springframework.batch.item.ExecutionContext)}, however you
 * should set <code>saveState=false</code> if used in a multi-threaded
 * environment (no restart available).
 * </p>
 *
 * @param <T> type of entity to load
 *
 * @author Michael Minella
 * @author Mahmoud Ben Hassine
 * @author Gerrit Meier
 */
public class Neo4jItemReader<T> extends AbstractPaginatedDataItemReader<T> implements InitializingBean {

	private final Log logger = LogFactory.getLog(getClass());

	private Neo4jTemplate neo4jTemplate;

	private StatementBuilder.OngoingReadingAndReturn statement;

	private Class<T> targetType;

	private Map<String, Object> parameterValues;

	/**
	 * Optional parameters to be used in the cypher query.
	 *
	 * @param parameterValues the parameter values to be used in the cypher query
	 */
	public void setParameterValues(Map<String, Object> parameterValues) {
		this.parameterValues = parameterValues;
	}

	/**
	 * Cypher-DSL's {@link org.neo4j.cypherdsl.core.StatementBuilder.OngoingReadingAndReturn} statement
	 * without skip and limit segments. Those will get added by the pagination mechanism later.
	 *
	 * @param statement the Cypher-DSL statement-in-construction.
	 */
	public void setStatement(StatementBuilder.OngoingReadingAndReturn statement) {
		this.statement = statement;
	}

	/**
	 * Establish the Neo4jTemplate for the reader.
	 *
	 * @param neo4jTemplate the template to use for the reader.
	 */
	public void setNeo4jTemplate(Neo4jTemplate neo4jTemplate) {
		this.neo4jTemplate = neo4jTemplate;
	}

	/**
	 * The object type to be returned from each call to {@link #read()}
	 *
	 * @param targetType the type of object to return.
	 */
	public void setTargetType(Class<T> targetType) {
		this.targetType = targetType;
	}

	private Statement generateStatement() {
		Statement builtStatement = statement
				.skip(page * pageSize)
				.limit(pageSize)
				.build();
		if (logger.isDebugEnabled()) {
			logger.debug(Renderer.getDefaultRenderer().render(builtStatement));
		}

		return builtStatement;
	}

	/**
	 * Checks mandatory properties
	 *
	 * @see InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.state(neo4jTemplate != null, "A Neo4jTemplate is required");
		Assert.state(targetType != null, "The type to be returned is required");
		Assert.state(statement != null, "A statement is required");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Iterator<T> doPageRead() {
		return neo4jTemplate.findAll(generateStatement(), parameterValues, targetType).iterator();
	}
}
