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
import org.mockito.internal.verification.Times;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Record;
import org.springframework.batch.item.Chunk;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.neo4j.core.Neo4jTemplate;
import org.springframework.data.neo4j.core.convert.Neo4jPersistentPropertyConverter;
import org.springframework.data.neo4j.core.mapping.*;
import org.springframework.data.util.TypeInformation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

public class Neo4jItemWriterTests {

	private Neo4jItemWriter<MyEntity> writer;

	private Neo4jTemplate neo4jTemplate;
	private Driver neo4jDriver;
	private Neo4jMappingContext neo4jMappingContext;

	@BeforeEach
	void setup() {
		neo4jTemplate = mock(Neo4jTemplate.class);
		neo4jDriver = mock(Driver.class);
		neo4jMappingContext = mock(Neo4jMappingContext.class);
	}

	@Test
	public void testAfterPropertiesSet() {

		writer = new Neo4jItemWriter<>();

		try {
			writer.afterPropertiesSet();
			fail("Neo4jTemplate was not set but exception was not thrown.");
		} catch (IllegalStateException iae) {
			assertEquals("A Neo4jTemplate is required", iae.getMessage());
		} catch (Throwable t) {
			fail("Wrong exception was thrown.");
		}

		writer.setNeo4jTemplate(this.neo4jTemplate);

		try {
			writer.afterPropertiesSet();
			fail("Neo4jMappingContext was not set but exception was not thrown.");
		} catch (IllegalStateException iae) {
			assertEquals("A Neo4jMappingContext is required", iae.getMessage());
		} catch (Throwable t) {
			fail("Wrong exception was thrown.");
		}

		writer.setNeo4jMappingContext(this.neo4jMappingContext);

		try {
			writer.afterPropertiesSet();
			fail("Neo4jDriver was not set but exception was not thrown.");
		} catch (IllegalStateException iae) {
			assertEquals("A Neo4j driver is required", iae.getMessage());
		} catch (Throwable t) {
			fail("Wrong exception was thrown.");
		}

		writer.setNeo4jDriver(this.neo4jDriver);

		writer.afterPropertiesSet();
	}

	@Test
	public void testWriteNoItems() {
		writer = new Neo4jItemWriter<>();

		writer.setNeo4jTemplate(this.neo4jTemplate);
		writer.setNeo4jDriver(this.neo4jDriver);
		writer.setNeo4jMappingContext(this.neo4jMappingContext);
		writer.afterPropertiesSet();

		writer.write(Chunk.of());

		verifyNoInteractions(this.neo4jTemplate);
	}

	@Test
	public void testWriteItems() {
		writer = new Neo4jItemWriter<>();

		writer.setNeo4jTemplate(this.neo4jTemplate);
		writer.setNeo4jDriver(this.neo4jDriver);
		writer.setNeo4jMappingContext(this.neo4jMappingContext);
		writer.afterPropertiesSet();

		writer.write(Chunk.of(new MyEntity("foo"), new MyEntity("bar")));

		verify(this.neo4jTemplate).saveAll(List.of(new MyEntity("foo"), new MyEntity("bar")));
	}

	@Test
	public void testDeleteItems() {
		TypeInformation<MyEntity> typeInformation = TypeInformation.of(MyEntity.class);
		NodeDescription<MyEntity> entity = new TestEntity<>(typeInformation);
		when(neo4jMappingContext.getNodeDescription(MyEntity.class)).thenAnswer(invocationOnMock -> entity);
		when(neo4jDriver.executableQuery(anyString())).thenReturn(new ExecutableQuery() {
			@Override
			public ExecutableQuery withParameters(Map<String, Object> parameters) {
				return this;
			}

			@Override
			public ExecutableQuery withConfig(QueryConfig config) {
				return null;
			}

			@Override
			public <A, R, T> T execute(Collector<Record, A, R> recordCollector, ResultFinisher<R, T> resultFinisher) {
				return null;
			}
		});

		writer = new Neo4jItemWriter<>();

		writer.setNeo4jTemplate(this.neo4jTemplate);
		writer.setNeo4jDriver(this.neo4jDriver);
		writer.setNeo4jMappingContext(this.neo4jMappingContext);
		writer.afterPropertiesSet();

		writer.setDelete(true);

		Chunk<MyEntity> myEntities = Chunk.of(new MyEntity("id1"), new MyEntity("id2"));
		writer.write(myEntities);

		verify(this.neo4jDriver, new Times(2)).executableQuery("MATCH (MyEntity) WHERE MyEntity.idField = $id DETACH DELETE MyEntity");
	}

	private record MyEntity(String idField) {
	}

	private static class TestEntity<T> extends BasicPersistentEntity<T, Neo4jPersistentProperty>
			implements Neo4jPersistentEntity<T> {

		public TestEntity(TypeInformation<T> information) {
			super(information);
			addPersistentProperty(new Neo4jPersistentProperty() {
				@Override
				public Neo4jPersistentPropertyConverter<?> getOptionalConverter() {
					return null;
				}

				@Override
				public boolean isEntityWithRelationshipProperties() {
					return false;
				}

				@Override
				public PersistentEntity<?, Neo4jPersistentProperty> getOwner() {
					return null;
				}

				@Override
				public String getName() {
					return "idField";
				}

				@Override
				public Class<?> getType() {
					return String.class;
				}

				@Override
				public TypeInformation<?> getTypeInformation() {
					return TypeInformation.of(String.class);
				}

				@Override
				public Iterable<? extends TypeInformation<?>> getPersistentEntityTypeInformation() {
					return null;
				}

				@Override
				public Method getGetter() {
					return null;
				}

				@Override
				public Method getSetter() {
					return null;
				}

				@Override
				public Method getWither() {
					return null;
				}

				@Override
				public Field getField() {
					try {
						return MyEntity.class.getDeclaredField("idField");
					} catch (NoSuchFieldException e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public String getSpelExpression() {
					return null;
				}

				@Override
				public Association<Neo4jPersistentProperty> getAssociation() {
					return null;
				}

				@Override
				public boolean isEntity() {
					return false;
				}

				@Override
				public boolean isIdProperty() {
					return true;
				}

				@Override
				public boolean isVersionProperty() {
					return false;
				}

				@Override
				public boolean isCollectionLike() {
					return false;
				}

				@Override
				public boolean isMap() {
					return false;
				}

				@Override
				public boolean isArray() {
					return false;
				}

				@Override
				public boolean isTransient() {
					return false;
				}

				@Override
				public boolean isWritable() {
					return true;
				}

				@Override
				public boolean isReadable() {
					return true;
				}

				@Override
				public boolean isImmutable() {
					return false;
				}

				@Override
				public boolean isAssociation() {
					return false;
				}

				@Override
				public Class<?> getComponentType() {
					return null;
				}

				@Override
				public Class<?> getRawType() {
					return String.class;
				}

				@Override
				public Class<?> getMapValueType() {
					return null;
				}

				@Override
				public Class<?> getActualType() {
					return String.class;
				}

				@Override
				public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
					return null;
				}

				@Override
				public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
					return null;
				}

				@Override
				public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
					return false;
				}

				@Override
				public boolean usePropertyAccess() {
					return false;
				}

				@Override
				public Class<?> getAssociationTargetType() {
					return null;
				}

				@Override
				public TypeInformation<?> getAssociationTargetTypeInformation() {
					return null;
				}

				@Override
				public String getFieldName() {
					return null;
				}

				@Override
				public String getPropertyName() {
					return null;
				}

				@Override
				public boolean isInternalIdProperty() {
					return false;
				}

				@Override
				public boolean isRelationship() {
					return false;
				}

				@Override
				public boolean isComposite() {
					return false;
				}
			});
		}

		@Override
		public Optional<Neo4jPersistentProperty> getDynamicLabelsProperty() {
			return Optional.empty();
		}

		@Override
		public boolean isRelationshipPropertiesEntity() {
			return false;
		}

		@Override
		public String getPrimaryLabel() {
			return "MyEntity";
		}

		@Override
		public String getMostAbstractParentLabel(NodeDescription<?> mostAbstractNodeDescription) {
			return null;
		}

		@Override
		public List<String> getAdditionalLabels() {
			return null;
		}

		@Override
		public Class<T> getUnderlyingClass() {
			return null;
		}

		@Override
		public IdDescription getIdDescription() {
			return IdDescription.forAssignedIds(Cypher.name("thing"), "idField");
		}

		@Override
		public Collection<GraphPropertyDescription> getGraphProperties() {
			return null;
		}

		@Override
		public Collection<GraphPropertyDescription> getGraphPropertiesInHierarchy() {
			return null;
		}

		@Override
		public Optional<GraphPropertyDescription> getGraphProperty(String fieldName) {
			return Optional.empty();
		}

		@Override
		public Collection<RelationshipDescription> getRelationships() {
			return null;
		}

		@Override
		public Collection<RelationshipDescription> getRelationshipsInHierarchy(Predicate<PropertyFilter.RelaxedPropertyPath> propertyPredicate) {
			return null;
		}

		@Override
		public void addChildNodeDescription(NodeDescription<?> child) {

		}

		@Override
		public Collection<NodeDescription<?>> getChildNodeDescriptionsInHierarchy() {
			return null;
		}

		@Override
		public void setParentNodeDescription(NodeDescription<?> parent) {

		}

		@Override
		public NodeDescription<?> getParentNodeDescription() {
			return null;
		}

		@Override
		public boolean containsPossibleCircles(Predicate<PropertyFilter.RelaxedPropertyPath> includeField) {
			return false;
		}

		@Override
		public boolean describesInterface() {
			return false;
		}
	}
}
