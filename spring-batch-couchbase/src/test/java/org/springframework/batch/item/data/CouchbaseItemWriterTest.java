package org.springframework.batch.item.data;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Arrays;
import java.util.List;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public class CouchbaseItemWriterTest {
	
	private CouchbaseItemWriter<Object> writer;
	
	@Mock
	private CouchbaseOperations couchbaseOperations;
	
	private TransactionTemplate transactionTemplate;
	
	@Before
	public void setUp() throws Exception {
		initMocks(this);
		transactionTemplate = new TransactionTemplate(new ResourcelessTransactionManager());
		writer = new CouchbaseItemWriter<>(couchbaseOperations);
		writer.afterPropertiesSet();
	}
	
	@After
	public void tearDown() {
		transactionTemplate = null;
		writer = null;
	}

	@Test(expected=IllegalArgumentException.class)
	public void shouldFailCouchbaseOperationsAssertion() throws Exception {
		
		try {
			new CouchbaseItemWriter<>(null).afterPropertiesSet();
			fail("Assertion should have thrown exception on null CouchbaseOperations");
		}catch(IllegalArgumentException e) {
			assertEquals("A CouchbaseOperations implementation is required.", e.getMessage());
			throw e;
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailPersistToAssertion() throws Exception {
		
		try {
			CouchbaseItemWriter<Object> writer = new CouchbaseItemWriter<>(couchbaseOperations);
			writer.setPersistTo(null);
			writer.afterPropertiesSet();
			fail("Assertion should have thrown exception on null PersistTo enum constant");
		}catch(IllegalArgumentException e) {
			assertEquals("A valid constant value is required for persistTo property. Allowed values are ".
						  concat(Arrays.toString(PersistTo.values())), e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred");
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldFailReplicateToAssertion() throws Exception {
		
		try {
			CouchbaseItemWriter<Object> writer = new CouchbaseItemWriter<>(couchbaseOperations);
			writer.setReplicateTo(null);
			writer.afterPropertiesSet();
			fail("Assertion should have thrown exception on null ReplicateTo enum constant");
		}catch(IllegalArgumentException e) {
			assertEquals("A valid constant value is required for replicateTo property. Allowed values are ".
						  concat(Arrays.toString(ReplicateTo.values())), e.getMessage());
			throw e;
		}catch (Exception e) {
			fail("unexpected error occurred");
		}
	}
	
	@Test
	public void shouldNotWriteWhenNoTransactionIsActiveAndNoItem() throws Exception {

		writer.write(null);
		verifyZeroInteractions(couchbaseOperations);

		writer.write(asList());
		verifyZeroInteractions(couchbaseOperations);
	}
	
	@Test
	public void shouldInsertItemWhenNoTransactionIsActive() throws Exception {
		
		List<Object> items = asList(new Object());
		
		writer.write(items);

		verify(couchbaseOperations).insert(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
	
	@Test
	public void shouldOverrideItemWhenNoTransactionIsActive() throws Exception {
		
		List<Object> items = asList(new Object());
		
		writer.setOverrideDocuments(true);
		writer.write(items);
		
		verify(couchbaseOperations).save(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
	
	@Test
	public void shouldInsertItemWhenInTransaction() throws Exception {

		final List<Object> items = asList(new Object());
		
		transactionTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				try {
					writer.write(items);
				} catch (Exception e) {
					fail("An error occurred while writing: " + e.getMessage());
				}

				return null;
			}
		});

		verify(couchbaseOperations).insert(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
	
	@Test
	public void shouldOverrideItemWhenInTransaction() throws Exception {

		final List<Object> items = asList(new Object());
		
		writer.setOverrideDocuments(true);
		
		transactionTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				try {
					writer.write(items);
				} catch (Exception e) {
					fail("An error occurred while writing: " + e.getMessage());
				}

				return null;
			}
		});

		verify(couchbaseOperations).save(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
	
	@Test
	public void shouldNotInsertItemWhenTransactionFails() throws Exception {

		final List<Object> items = asList(new Object());

		try {
			transactionTemplate.execute(new TransactionCallback<Void>() {

				@Override
				public Void doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception ignore) {
						fail("unexpected error occurred");
					}
					throw new RuntimeException("rollback");
				}
			});
		} catch (RuntimeException re) {
			// ignore
		} catch (Throwable t) {
			fail("Unexpected error occurred");
		}

		verifyZeroInteractions(couchbaseOperations);
	}
	
	@Test
	public void shouldNotOverrideItemWhenTransactionFails() throws Exception {

		final List<Object> items = asList(new Object());

		writer.setOverrideDocuments(true);
		
		try {
			transactionTemplate.execute(new TransactionCallback<Void>() {

				@Override
				public Void doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception ignore) {
						fail("unexpected error occurred");
					}
					throw new RuntimeException("rollback");
				}
			});
		} catch (RuntimeException re) {
			// ignore
		} catch (Throwable t) {
			fail("Unexpected error occurred");
		}

		verifyZeroInteractions(couchbaseOperations);
	}
	
	@Test
	public void shouldNotInsertItemWhenTransactionIsReadOnly() throws Exception {

		final List<Object> items = asList(new Object());

		try {

			transactionTemplate.setReadOnly(true);
			transactionTemplate.execute(new TransactionCallback<Void>() {

				@Override
				public Void doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception ignore) {
						fail("unexpected error occurred");
					}
					return null;
				}
			});
		} catch (Throwable t) {
			fail("unexpected error occurred");
		}

		verifyZeroInteractions(couchbaseOperations);
	}
	
	@Test
	public void shouldNotOverrideItemWhenTransactionIsReadOnly() throws Exception {

		final List<Object> items = asList(new Object());

		writer.setOverrideDocuments(true);
		
		try {

			transactionTemplate.setReadOnly(true);
			transactionTemplate.execute(new TransactionCallback<Void>() {

				@Override
				public Void doInTransaction(TransactionStatus status) {
					try {
						writer.write(items);
					} catch (Exception ignore) {
						fail("unexpected error occurred");
					}
					return null;
				}
			});
		} catch (Throwable t) {
			fail("unexpected error occurred");
		}

		verifyZeroInteractions(couchbaseOperations);
	}
	
	@Test
	public void shouldRemoveItemWhenNoTransactionIsActive() throws Exception {

		final List<Object> items = asList(new Object());
		
		writer.setDelete(true);		
		writer.write(items);
		
		verify(couchbaseOperations).remove(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
	
	@Test
	public void shouldRemoveItemWhenInTransaction() throws Exception {

		final List<Object> items = asList(new Object());
		
		writer.setDelete(true);

		transactionTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				try {
					writer.write(items);
				} catch (Exception e) {
					fail("An error occurred while writing: " + e.getMessage());
				}

				return null;
			}
		});
		
		verify(couchbaseOperations).remove(items.iterator().next(), PersistTo.ZERO, ReplicateTo.ZERO);
	}
}