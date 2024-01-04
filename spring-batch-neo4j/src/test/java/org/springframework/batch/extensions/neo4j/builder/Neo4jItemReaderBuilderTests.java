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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.StatementBuilder;
import org.springframework.batch.extensions.neo4j.Neo4jItemReader;
import org.springframework.data.neo4j.core.Neo4jTemplate;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Glenn Renfro
 * @author Gerrit Meier
 */
public class Neo4jItemReaderBuilderTests {

	private List<String> result;
	private Neo4jTemplate neo4jTemplate;
	private StatementBuilder.OngoingReadingAndReturn dummyStatement = Cypher.match(Cypher.anyNode()).returning(Cypher.anyNode());

	@SuppressWarnings("unchecked")
	@BeforeEach
	void setup() {
		result = mock(List.class);
		neo4jTemplate = mock(Neo4jTemplate.class);
	}

	@Test
	public void testFullyQualifiedItemReader() throws Exception {
		dummyStatement = Cypher.match(Cypher.anyNode()).returning(Cypher.anyNode());
		Neo4jItemReader<String> itemReader = new Neo4jItemReaderBuilder<String>()
				.neo4jTemplate(this.neo4jTemplate)
				.targetType(String.class)
				.statement(dummyStatement)
				.pageSize(50).name("bar")
				.build();

		when(this.neo4jTemplate.findAll(any(Statement.class), any(), eq(String.class)))
						.thenReturn(result);
		when(result.iterator()).thenReturn(Arrays.asList("foo", "bar", "baz").iterator());

		assertEquals("foo", itemReader.read());
		assertEquals("bar", itemReader.read());
		assertEquals("baz", itemReader.read());
	}

	@Test
	public void testCurrentSize() throws Exception {
		Neo4jItemReader<String> itemReader = new Neo4jItemReaderBuilder<String>()
				.neo4jTemplate(this.neo4jTemplate)
				.targetType(String.class)
				.statement(dummyStatement)
				.pageSize(50).name("bar")
				.currentItemCount(0)
				.maxItemCount(1)
				.build();

		when(this.neo4jTemplate.findAll(any(Statement.class), any(), eq(String.class)))
				.thenReturn(result);
		when(result.iterator()).thenReturn(Arrays.asList("foo", "bar", "baz").iterator());

		assertEquals("foo", itemReader.read());
		assertNull(itemReader.read());
	}


	@Test
	public void testNoSessionFactory() {
		try {
			new Neo4jItemReaderBuilder<String>()
					.targetType(String.class)
					.pageSize(50)
					.name("bar").build();

			fail("IllegalArgumentException should have been thrown");
		}
		catch (IllegalArgumentException iae) {
			assertEquals("neo4jTemplate is required.", iae.getMessage());
		}
	}

	@Test
	public void testZeroPageSize() {
		validateExceptionMessage(new Neo4jItemReaderBuilder<String>()
				.neo4jTemplate(this.neo4jTemplate)
				.targetType(String.class)
				.statement(dummyStatement)
				.pageSize(0)
				.name("foo"),
				"pageSize must be greater than zero");
	}

	@Test
	public void testZeroMaxItemCount() {
		validateExceptionMessage(new Neo4jItemReaderBuilder<String>()
						.neo4jTemplate(this.neo4jTemplate)
						.targetType(String.class)
						.statement(dummyStatement)
						.pageSize(5)
						.maxItemCount(0)
						.name("foo"),
				"maxItemCount must be greater than zero");
	}

	@Test
	public void testCurrentItemCountGreaterThanMaxItemCount() {
		validateExceptionMessage(new Neo4jItemReaderBuilder<String>()
						.neo4jTemplate(this.neo4jTemplate)
						.targetType(String.class)
						.statement(dummyStatement)
						.pageSize(5)
						.maxItemCount(5)
						.currentItemCount(6)
						.name("foo"),
				"maxItemCount must be greater than currentItemCount");
	}

	@Test
	public void testNullName() {
		validateExceptionMessage(
				new Neo4jItemReaderBuilder<String>()
						.neo4jTemplate(this.neo4jTemplate)
						.targetType(String.class)
						.statement(dummyStatement)
						.pageSize(50),
				"A name is required when saveState is set to true");

		// tests that name is not required if saveState is set to false.
		new Neo4jItemReaderBuilder<String>()
				.neo4jTemplate(this.neo4jTemplate)
				.targetType(String.class)
				.statement(dummyStatement)
				.saveState(false)
				.pageSize(50)
				.build();
	}

	@Test
	public void testNullTargetType() {
		validateExceptionMessage(
				new Neo4jItemReaderBuilder<String>()
						.neo4jTemplate(this.neo4jTemplate)
						.statement(dummyStatement)
						.pageSize(50)
						.name("bar"),
				"targetType is required.");
	}

	@Test
	public void testNullStatement() {
		validateExceptionMessage(
				new Neo4jItemReaderBuilder<String>()
						.neo4jTemplate(this.neo4jTemplate)
						.targetType(String.class)
						.pageSize(50).name("bar"),
				"statement is required.");
	}

	private void validateExceptionMessage(Neo4jItemReaderBuilder<?> builder, String message) {
		try {
			builder.build();
			fail("IllegalArgumentException should have been thrown");
		}
		catch (IllegalArgumentException iae) {
			assertEquals(message, iae.getMessage());
		}
	}
}
