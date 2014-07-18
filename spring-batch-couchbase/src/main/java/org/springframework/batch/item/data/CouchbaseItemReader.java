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

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.util.Assert.hasLength;
import static org.springframework.util.Assert.notNull;
import static org.springframework.util.ClassUtils.getShortName;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.core.CouchbaseOperations;

import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

/**
 * <p>
 * Restartable {@link ItemReader} that reads documents from Couchbase
 * via a paging technique.
 * </p>
 *
 * <p>
 * It executes the query object {@link Query} to retrieve the requested
 * documents. Additional pages are requested as needed to provide data 
 * when the {@link #read()} method is called. If the limit is not set on the 
 * {@link Query} object, the default limit will be applied as specified in 
 * {@link AbstractPaginatedDataItemReader#pageSize}
 * </p>
 *
 * <p>
 * The implementation is thread-safe between calls to
 * {@link #open(ExecutionContext)}, but remember to use <code>saveState=false</code>
 * if used in a multi-threaded client (no restart available).
 * </p>
 *
 *
 * @author Hasnain Javed
 * @since  3.x.x
 * @param <T> Type of item to be read
 */
public class CouchbaseItemReader<T> extends AbstractPaginatedDataItemReader<T> implements InitializingBean {
	
	private final Logger logger;
	
	private CouchbaseOperations couchbaseOperations;
	
	private Query query;
	
	private String designDocument;
	private String view;
	
	private Class<? extends T> targetType;
	
	public CouchbaseItemReader() {
		setName(getShortName(getClass()));
		logger = getLogger(getClass());
	}
	
	/**
	 * Used to perform operations against the Couchbase instance.  Also
	 * handles the mapping of documents to objects.
	 *
	 * @param couchbaseOperations the CouchbaseOperations instance to use
	 * @see CouchbaseOperations
	 */
	public void setCouchbaseOperations(CouchbaseOperations couchbaseOperations) {
		this.couchbaseOperations = couchbaseOperations;
	}
	
	/**
	 * Used to fetch documents from Couchbase.
	 *
	 * @param query the query to be executed
	 * @see Query
	 */
	public void setQuery(Query query) {
		this.query = query;
	}

	/**
	 * @param designDocument the name of the design document
	 */
	public void setDesignDocument(String designDocument) {
		this.designDocument = designDocument;
	}

	/**
	 * @param view the name of the view
	 */
	public void setView(String view) {
		this.view = view;
	}

	/**
	 * The type of object to be returned for each {@link #read()} call.
	 *
	 * @param type the type of object to return
	 */
	public void setTargetType(Class<? extends T> targetType) {
		this.targetType = targetType;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		notNull(couchbaseOperations, "A CouchbaseOperations implementation is required.");		
		notNull(query, "A valid query is required.");
		notNull(targetType, "A target type to convert the input into is required.");
		hasLength(designDocument, "A design document name is required.");
		hasLength(view, "A view name is required.");
		
		// default value is -1 (unlimited)
		if(query.getLimit() < 0) {
			logger.debug("setting default page size to {}", pageSize);
			query.setLimit(pageSize);
		}
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected Iterator<T> doPageRead() {
		
		Iterator<T> iterator = null;
		
		logger.debug("executing query {} with design document {} in view {}", query, designDocument, view);
		
		if(query.willReduce()) {
			ViewResponse response = couchbaseOperations.queryView(designDocument, view, query);
			iterator = getItems(response);
		}else {
			iterator = (Iterator<T>)couchbaseOperations.findByView(designDocument, view, query, targetType).iterator();
		}
		
		return iterator;
	}
	
	private Iterator<T> getItems(ViewResponse response) {
		
		List<T> items = new ArrayList<T>(response.size());
		
	    for( ViewRow row : response) {
	    	String id = row.getId();
	    	logger.debug("fetching document with id {}", id);
	    	T item = couchbaseOperations.findById(id, targetType);
	    	items.add(item);
	    }
	    
	    return items.iterator();
	}
}