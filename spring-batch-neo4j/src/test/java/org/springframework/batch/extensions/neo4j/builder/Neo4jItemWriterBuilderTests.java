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
import org.neo4j.cypherdsl.core.Functions;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.springframework.batch.extensions.neo4j.Neo4jItemWriter;
import org.springframework.batch.item.Chunk;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.data.neo4j.core.mapping.IdDescription;
import org.springframework.data.neo4j.core.mapping.Neo4jMappingContext;
import org.springframework.data.neo4j.core.mapping.Neo4jPersistentEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

/**
 * @author Glenn Renfro
 * @author Gerrit Meier
 */
public class Neo4jItemWriterBuilderTests {

    private Neo4jTemplate neo4jTemplate;

    private Driver neo4jDriver;

    private Neo4jMappingContext neo4jMappingContext;

    @BeforeEach
    void setup() {
        neo4jDriver = mock(Driver.class);
        neo4jTemplate = mock(Neo4jTemplate.class);
        neo4jMappingContext = mock(Neo4jMappingContext.class);
    }

    @Test
    public void testBasicWriter() {
        Neo4jItemWriter<String> writer = new Neo4jItemWriterBuilder<String>()
            .neo4jTemplate(this.neo4jTemplate)
            .neo4jDriver(this.neo4jDriver)
            .neo4jMappingContext(this.neo4jMappingContext)
            .build();

        Chunk<String> items = Chunk.of("foo", "bar");
        writer.write(items);

        verify(this.neo4jTemplate).saveAll(items.getItems());
        verify(this.neo4jDriver, never()).executableQuery(anyString());
    }

    @Test
    public void testBasicDelete() {
        Neo4jItemWriter<String> writer = new Neo4jItemWriterBuilder<String>()
            .delete(true)
            .neo4jMappingContext(this.neo4jMappingContext)
            .neo4jTemplate(this.neo4jTemplate)
            .neo4jDriver(neo4jDriver)
            .build();

        // needs some mocks to create the testable environment
        Neo4jPersistentEntity<?> persistentEntity = mock(Neo4jPersistentEntity.class);
        IdentifierAccessor identifierAccessor = mock(IdentifierAccessor.class);
        IdDescription idDescription = mock(IdDescription.class);
        ExecutableQuery executableQuery = mock(ExecutableQuery.class);
        when(identifierAccessor.getRequiredIdentifier()).thenReturn("someId");
        when(idDescription.asIdExpression(anyString())).thenReturn(Functions.id(Cypher.anyNode()));
        when(executableQuery.withParameters(any())).thenReturn(executableQuery);
        when(persistentEntity.getIdentifierAccessor(any())).thenReturn(identifierAccessor);
        when(persistentEntity.getPrimaryLabel()).thenReturn("SomeLabel");
        when(persistentEntity.getIdDescription()).thenReturn(idDescription);
        when(this.neo4jMappingContext.getNodeDescription(any(Class.class))).thenAnswer(invocationOnMock -> persistentEntity);
        when(this.neo4jDriver.executableQuery(anyString())).thenReturn(executableQuery);

        Chunk<String> items = Chunk.of("foo", "bar");

        writer.write(items);

        verify(this.neo4jDriver, times(2)).executableQuery(anyString());
        verify(this.neo4jTemplate, never()).save(items);
    }

    @Test
    public void testNoNeo4jDriver() {
        try {
            new Neo4jItemWriterBuilder<String>().neo4jTemplate(neo4jTemplate).neo4jMappingContext(neo4jMappingContext).build();
            fail("Neo4jTemplate was not set but exception was not thrown.");
        } catch (IllegalArgumentException iae) {
            assertEquals("neo4jDriver is required.", iae.getMessage());
        }
    }

    @Test
    public void testNoMappingContextFactory() {
        try {
            new Neo4jItemWriterBuilder<String>().neo4jTemplate(neo4jTemplate).neo4jDriver(neo4jDriver).build();
            fail("Neo4jTemplate was not set but exception was not thrown.");
        } catch (IllegalArgumentException iae) {
            assertEquals("neo4jMappingContext is required.", iae.getMessage());
        }
    }

    @Test
    public void testNoNeo4jTemplate() {
        try {
            new Neo4jItemWriterBuilder<String>().build();
            fail("Neo4jTemplate was not set but exception was not thrown.");
        } catch (IllegalArgumentException iae) {
            assertEquals("neo4jTemplate is required.", iae.getMessage());
        }
    }

}
