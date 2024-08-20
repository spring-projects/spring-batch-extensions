/*
 * Copyright 2013-2021 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.Statement;
import org.springframework.data.neo4j.core.Neo4jTemplate;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Neo4jItemReaderTests {

    private Neo4jTemplate neo4jTemplate;

    @BeforeEach
    void setup() {
        neo4jTemplate = mock(Neo4jTemplate.class);
    }

    private Neo4jItemReader<String> buildSessionBasedReader() {
        Neo4jItemReader<String> reader = new Neo4jItemReader<>();

        reader.setNeo4jTemplate(this.neo4jTemplate);
        reader.setTargetType(String.class);
        Node n = Cypher.anyNode().named("n");
        reader.setStatement(Cypher.match(n).returning(n));
        reader.setPageSize(50);
        reader.afterPropertiesSet();

        return reader;
    }

    @Test
    public void testAfterPropertiesSet() {

        Neo4jItemReader<String> reader = new Neo4jItemReader<>();

        try {
            reader.afterPropertiesSet();
            fail("SessionFactory was not set but exception was not thrown.");
        } catch (IllegalStateException iae) {
            assertEquals("A Neo4jTemplate is required", iae.getMessage());
        } catch (Throwable t) {
            fail("Wrong exception was thrown:" + t);
        }

        reader.setNeo4jTemplate(this.neo4jTemplate);

        try {
            reader.afterPropertiesSet();
            fail("Target Type was not set but exception was not thrown.");
        } catch (IllegalStateException iae) {
            assertEquals("The type to be returned is required", iae.getMessage());
        } catch (Throwable t) {
            fail("Wrong exception was thrown:" + t);
        }

        reader.setTargetType(String.class);

        reader.setStatement(Cypher.match(Cypher.anyNode()).returning(Cypher.anyNode()));

        reader.afterPropertiesSet();

        reader = new Neo4jItemReader<>();
        reader.setNeo4jTemplate(this.neo4jTemplate);
        reader.setTargetType(String.class);
        reader.setStatement(Cypher.match(Cypher.anyNode()).returning(Cypher.anyNode()));

        reader.afterPropertiesSet();
    }

    @Test
    public void testNullResultsWithSession() {

        Neo4jItemReader<String> itemReader = buildSessionBasedReader();

        ArgumentCaptor<Statement> query = ArgumentCaptor.forClass(Statement.class);

        when(this.neo4jTemplate.findAll(query.capture(), isNull(), eq(String.class))).thenReturn(List.of());

        assertFalse(itemReader.doPageRead().hasNext());
        Node node = Cypher.anyNode().named("n");
        assertEquals(Cypher.match(node).returning(node).skip(0).limit(50).build().getCypher(), query.getValue().getCypher());

    }

    @Test
    public void testNoResultsWithSession() {
        Neo4jItemReader<String> itemReader = buildSessionBasedReader();
        ArgumentCaptor<Statement> query = ArgumentCaptor.forClass(Statement.class);

        when(this.neo4jTemplate.findAll(query.capture(), any(), eq(String.class))).thenReturn(List.of());

        assertFalse(itemReader.doPageRead().hasNext());
        Node node = Cypher.anyNode().named("n");
        assertEquals(Cypher.match(node).returning(node).skip(0).limit(50).build().getCypher(), query.getValue().getCypher());
    }

    @Test
    public void testResultsWithMatchAndWhereWithSession() {
        Neo4jItemReader<String> itemReader = buildSessionBasedReader();
        itemReader.afterPropertiesSet();

        when(this.neo4jTemplate.findAll(any(Statement.class), isNull(), eq(String.class))).thenReturn(Arrays.asList("foo", "bar", "baz"));

        assertTrue(itemReader.doPageRead().hasNext());
    }

}
