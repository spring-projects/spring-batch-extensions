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

import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.driver.Driver;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.data.neo4j.core.mapping.Neo4jPersistentEntity;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * A {@link ItemWriter} implementation that writes to a Neo4j database.
 * </p>
 *
 * <p>
 * This writer is thread-safe once all properties are set (normal singleton
 * behavior) so it can be used in multiple concurrent transactions.
 * </p>
 *
 * @param <T> type of the entity to write
 *
 * @author Michael Minella
 * @author Glenn Renfro
 * @author Mahmoud Ben Hassine
 * @author Gerrit Meier
 *
 */
public class Neo4jItemWriter<T> implements ItemWriter<T>, InitializingBean {

	private boolean delete = false;

	private Neo4jTemplate neo4jTemplate;
	private Neo4jMappingContext neo4jMappingContext;
	private Driver neo4jDriver;

	/**
	 * Boolean flag indicating whether the writer should save or delete the item at write
	 * time.
	 * @param delete true if write should delete item, false if item should be saved.
	 * Default is false.
	 */
	public void setDelete(boolean delete) {
		this.delete = delete;
	}

	/**
	 * Establish the neo4jTemplate for interacting with Neo4j.
	 * @param neo4jTemplate neo4jTemplate to be used.
	 */
	public void setNeo4jTemplate(Neo4jTemplate neo4jTemplate) {
		this.neo4jTemplate = neo4jTemplate;
	}

	/**
	 * Set the Neo4j driver to be used for the delete operation
	 * @param neo4jDriver configured Neo4j driver instance
	 */
	public void setNeo4jDriver(Driver neo4jDriver) {
		this.neo4jDriver = neo4jDriver;
	}

	/**
	 * Neo4jMappingContext needed for determine the id type of the entity instances.
	 *
	 * @param neo4jMappingContext initialized mapping context
	 */
	public void setNeo4jMappingContext(Neo4jMappingContext neo4jMappingContext) {
		this.neo4jMappingContext = neo4jMappingContext;
	}

	/**
	 * Checks mandatory properties
	 *
	 * @see InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.state(this.neo4jTemplate != null, "A Neo4jTemplate is required");
		Assert.state(this.neo4jMappingContext != null, "A Neo4jMappingContext is required");
		Assert.state(this.neo4jDriver != null, "A Neo4j driver is required");
	}

	/**
	 * Write all items to the data store.
	 *
	 * @see org.springframework.batch.item.ItemWriter#write(Chunk chunk)
	 */
		@Override
		public void write(@NonNull Chunk<? extends T> chunk) {
			if (!chunk.isEmpty()) {
					doWrite(chunk.getItems());
			}
		}

	/**
	 * Performs the actual write using the template.  This can be overridden by
	 * a subclass if necessary.
	 *
	 * @param items the list of items to be persisted.
	 */
	protected void doWrite(List<? extends T> items) {
		if(delete) {
			delete(items);
		}
		else {
			save(items);
		}
	}

	private void delete(List<? extends T> items) {
		for(T item : items) {
			// Figure out id field individually because different
			// id strategies could have been taken for classes within a
			// business model hierarchy.
			Neo4jPersistentEntity<?> nodeDescription = (Neo4jPersistentEntity<?>) this.neo4jMappingContext.getNodeDescription(item.getClass());
			Object identifier = nodeDescription.getIdentifierAccessor(item).getRequiredIdentifier();
			Node named = Cypher.anyNode().named(nodeDescription.getPrimaryLabel());
			Statement statement = Cypher.match(named)
					.where(nodeDescription.getIdDescription().asIdExpression(nodeDescription.getPrimaryLabel()).eq(Cypher.parameter("id")))
					.detachDelete(named).build();

			String renderedStatement = Renderer.getDefaultRenderer().render(statement);
			this.neo4jDriver.executableQuery(renderedStatement).withParameters(Map.of("id", identifier)).execute();
		}
	}

	private void save(List<? extends T> items) {
		this.neo4jTemplate.saveAll(items);
	}
}
