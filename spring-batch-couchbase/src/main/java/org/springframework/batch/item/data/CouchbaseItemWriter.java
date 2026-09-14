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

import static java.lang.String.valueOf;
import static java.util.UUID.randomUUID;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.transaction.support.TransactionSynchronizationManager.bindResource;
import static org.springframework.transaction.support.TransactionSynchronizationManager.getResource;
import static org.springframework.transaction.support.TransactionSynchronizationManager.hasResource;
import static org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive;
import static org.springframework.transaction.support.TransactionSynchronizationManager.registerSynchronization;
import static org.springframework.transaction.support.TransactionSynchronizationManager.unbindResource;
import static org.springframework.util.Assert.notNull;
import static org.springframework.util.CollectionUtils.isEmpty;

import java.util.Arrays;
import java.util.List;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;

/**
 * <p>
 * A {@link ItemWriter} implementation that writes to Couchbase store using an implementation of Spring Data's
 * {@link CouchbaseOperations}. Couchbase is not a transactional store and does not support transactions in the 
 * traditional sense of the word. Couchbase is ACID compliant on a per document basis and have concurrency/locking
 * controls. The strategy for writing data is similar to {@link MongoItemWriter}.
 * There is no roll back if an error occurs
 * </p>
 *
 * <p>
 * This writer is thread-safe once all properties are set (normal singleton behavior) so it can be used in multiple
 * concurrent transactions.
 * </p>
 *
 * @author Hasnain Javed
 * @since  3.x.x
 * @param <T> Type of item to be written
 *
 */
public class CouchbaseItemWriter<T> implements ItemWriter<T>, InitializingBean {
	
	private final String dataKey;
	
	private final Logger logger;
	
	private final CouchbaseOperations couchbaseOperations;
	
	private PersistTo persistTo;
	private ReplicateTo replicateTo;
	
	private boolean delete;
	private boolean overrideDocuments;
	
	public CouchbaseItemWriter(CouchbaseOperations couchbaseOperations) {
		super();		
		dataKey = valueOf(randomUUID());
		logger = getLogger(getClass());
		persistTo = PersistTo.ZERO;
		replicateTo = ReplicateTo.ZERO;
		delete = false;
		overrideDocuments = false;
		this.couchbaseOperations = couchbaseOperations;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		notNull(couchbaseOperations, "A CouchbaseOperations implementation is required.");
		notNull(persistTo, "A valid constant value is required for persistTo property. Allowed values are ".
							concat(Arrays.toString(PersistTo.values())));
		notNull(replicateTo, "A valid constant value is required for replicateTo property. Allowed values are ".
							  concat(Arrays.toString(ReplicateTo.values())));
	}
	
	/**
	 * A flag for removing items given to the writer. Default value is set to false indicating that the 
	 * items will not be removed.
	 *
	 * @param delete flag
	 */
	public void setDelete(boolean delete) {
		this.delete = delete;
	}
	
	/**
	 * A flag for overriding existing items given to the writer. Default value is set to false indicating
	 * that the items will be inserted. otherwise, the items will be overridden.
	 *
	 * @param override flag
	 */
	public void setOverrideDocuments(boolean overrideDocuments) {
		this.overrideDocuments = overrideDocuments;
	}
	
	/**
	 * Specifies value for persisting to node(s).
	 * Defaults to ZERO
	 * @param persistTo enum value
	 * @see PersistTo
	 */
	public void setPersistTo(PersistTo persistTo) {
		this.persistTo = persistTo;
	}

	/**
	 * Specifies value for replicating to node(s).
	 * Defaults to ZERO
	 * @param replicateTo enum value
	 * @see ReplicateTo
	 */
	public void setReplicateTo(ReplicateTo replicateTo) {
		this.replicateTo = replicateTo;
	}
	
	@Override
	public void write(List<? extends T> items) throws Exception {
		
		if(isActualTransactionActive()) {
			bufferItems(items);
		}else {
			writeItems(items);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void bufferItems(List<? extends T> items) {

		if(hasResource(dataKey)) {
			logger.debug("appending items to buffer under key {}", dataKey);
			List<T> buffer = (List<T>) getResource(dataKey);
			buffer.addAll(items);
		}else {
			logger.debug("adding items to buffer under key {}", dataKey);
			bindResource(dataKey, items);
			registerSynchronization(new TransactionSynchronizationCallbackImpl());
		}
	}

	/**
	 * Writes to Couchbase via the template.
	 * This can be overridden by a subclass if required.
	 *
	 * @param items the list of items to be added/removed to/from bucket.
	 */
	protected void writeItems(List<? extends T> items) {

		if(isEmpty(items)) {
			logger.warn("no items to write to couchbase. list is empty or null");
		}else {
			for(T item : items) {
				if(delete) {
					logger.debug("deleting item {}", item);
					couchbaseOperations.remove(item, persistTo, replicateTo);
				}else if(overrideDocuments) {
					logger.debug("overriding item {}", item);
					couchbaseOperations.save(item, persistTo, replicateTo);
				}else {
					logger.debug("inserting item {}", item);
					couchbaseOperations.insert(item, persistTo, replicateTo);
				}
			}
		}
	}
	
	private class TransactionSynchronizationCallbackImpl extends TransactionSynchronizationAdapter {
		@SuppressWarnings("unchecked")
		@Override
		public void beforeCommit(boolean readOnly) {

			List<T> items = (List<T>) getResource(dataKey);
			if(!isEmpty(items)) {
				if(!readOnly) {
					writeItems(items);
				}else{
					logger.warn("can not write items to couchbase as transaction is read only");
				}
			}else {
				logger.warn("no items to write to couchbase. list is empty or null");
			}
		}

		@Override
		public void afterCompletion(int status) {
			if(hasResource(dataKey)) {
				logger.debug("removing items from buffer under key {}", dataKey);
				unbindResource(dataKey);
			}
		}
	}
}