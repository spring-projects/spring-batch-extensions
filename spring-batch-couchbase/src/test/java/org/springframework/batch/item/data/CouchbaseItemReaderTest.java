/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.item.data;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.couchbase.core.CouchbaseOperations;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

public class CouchbaseItemReaderTest {
	
	@Mock
	private CouchbaseOperations couchbaseOperations;
	
	private CouchbaseItemReader<Object> reader;

	private Query query;
	
	private String view;
	private String designDocument;
	
	private Class<Object> targetType;
	
	@Before
	public void setUp() throws Exception {
		
		initMocks(this);
		
		view = "testView";
		designDocument = "testDocument";
		targetType = Object.class;
		query = new Query();
		reader = new CouchbaseItemReader<>();
		reader.setCouchbaseOperations(couchbaseOperations);
		reader.setDesignDocument(designDocument);
		reader.setView(view);
		reader.setQuery(query);
		reader.setTargetType(targetType);
		reader.afterPropertiesSet();
	}
	
	@After
	public void tearDown() {
		designDocument = null;
		view = null;
		targetType = null;
		query = null;
		couchbaseOperations = null;
		reader = null;
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailCouchbaseOperationsAssertion() throws Exception {
		
		try {
			new CouchbaseItemReader<>().afterPropertiesSet();
			fail("Assertion should have thrown exception on null CouchbaseOperations");
		}catch(IllegalArgumentException e) {
			assertEquals("A CouchbaseOperations implementation is required.", e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred"); 
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailQueryAssertion() throws Exception {
		
		try {
			reader.setQuery(null);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null query");
		}catch(IllegalArgumentException e) {
			assertEquals("A valid query is required.", e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred"); 
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailTargetTypeAssertion() throws Exception {
		
		try {
			reader.setTargetType(null);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null target type");
		}catch(IllegalArgumentException e) {
			assertEquals("A target type to convert the input into is required.", e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred"); 
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailDesignDocumentAssertion() throws Exception {
		
		try {
			reader.setDesignDocument(null);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null design document name");
		}catch(IllegalArgumentException e) {
			assertEquals("A design document name is required.", e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred"); 
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailViewAssertion() throws Exception {
		
		try {
			reader.setView(null);
			reader.afterPropertiesSet();
			fail("Assertion should have thrown exception on null view name");
		}catch(IllegalArgumentException e) {
			assertEquals("A view name is required.", e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred"); 
		}
	}
	
	@Test
	public void shouldSetDefaultLimitOnQuery() throws Exception {
		
		Query query = new Query();
		
		assertThat(query.getLimit(), equalTo(-1));
		
		reader.setQuery(query);
		reader.afterPropertiesSet();
		
		assertThat(query.getLimit(), equalTo(reader.pageSize));
	}
	
	@Test
	public void shouldRunReducedQuery() {
		
		String documentId = "111";
		
		query.setReduce(true);
		
		ViewResponse viewResponseMock = mock(ViewResponse.class);
		ViewRow viewRowMock = mock(ViewRow.class);
		
		List<ViewRow> rows = asList(viewRowMock);
		
		when(couchbaseOperations.queryView(designDocument, view, query)).thenReturn(viewResponseMock);
		when(viewResponseMock.size()).thenReturn(1);
		when(viewResponseMock.iterator()).thenReturn(rows.iterator());
		when(viewRowMock.getId()).thenReturn(documentId);
		when(couchbaseOperations.findById(documentId, targetType)).thenReturn(new Object());
		
		reader.doPageRead();
		
		verify(couchbaseOperations).queryView(designDocument, view, query);
		verify(viewResponseMock).size();
		verify(viewRowMock).getId();
		verify(couchbaseOperations).findById(documentId, targetType);
	}
	
	@Test
	public void shouldRunQuery() {
		
		when(couchbaseOperations.findByView(designDocument, view, query, targetType)).thenReturn(asList());

		reader.doPageRead();
		
		verify(couchbaseOperations).findByView(designDocument, view, query, targetType);
	}
}