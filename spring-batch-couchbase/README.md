# spring-batch-couchbase
=========
ItemReader and ItemWriter implementations for Couchbase using Spring Data Couchbase. Please read the [reference manual] for implementation details of
different methods to read/write to/from Couchbase as provided by [CouchbaseOperations] and other configuration details.

Please read the api documentation for [Query] for configuring query parameters.

## Configuration for reader/writer
=========
```
@Configuration
public class ReaderWriterConfig {

	@Bean
	public ItemReader<ClassToRead> couchbaseItemReader() {
	
	    CouchbaseItemReader<ClassToRead> reader = new CouchbaseItemReader<ClassToRead>();
	    reader.setCouchbaseOperations(couchbaseOperations());
	    reader.setQuery(query());
	    reader.setDesignDocument("designDocumentName");
	    reader.setView("viewName");
	    reader.setTargetType(ClassToRead.class);
	    
	    return reader;
	}
	
	@Bean
	public ItemWriter<ClassToWrite> couchbaseItemWriter() {
	
		CouchbaseItemWriter<ClassToWrite> writer = new CouchbaseItemWriter<ClassToWrite>(couchbaseOperations());
		
		// Optional
		// writer.setDelete(true|false); (defaults to false)
		// writer.setOverrideDocuments(true|false); (defaults to false)
		// writer.setPersistTo(ZERO|MASTER|ONE|TWO|THREE|FOUR); (defaults to PersistTo.ZERO)
		// writer.setReplicateTo(ZERO|ONE|TWO|THREE); (defaults to ReplicateTo.ZERO)
		
	    return writer;
	}
	 
	@Bean
	public Query query() {
	     
	    Query query = new Query();
	    // configure the query as required (there are 17 query parameters)
	     
	    return query;
	}
	 
	@Bean
	public CouchbaseOperations couchbaseOperations() {
	    // configure and return Couchbase template
	}
}
```

##### NOTE
The limit attribute from the Query object will be used for paged requests. Setting the page and pageSize fields (inherited from AbstractPaginatedDataItemReader) will have no effect.
If the limit attribute is not set on the Query object, then the pageSize (inherited from AbstractPaginatedDataItemReader) will be set as the limit.

[reference manual]:http://docs.spring.io/spring-data/couchbase/docs/1.1.1.RELEASE/reference/html/
[CouchbaseOperations]:http://docs.spring.io/spring-data/couchbase/docs/1.1.1.RELEASE/api/org/springframework/data/couchbase/core/CouchbaseOperations.html
[Query]:http://www.couchbase.com/autodocs/couchbase-java-client-1.4.3/index.html?com/couchbase/client/protocol/views/Query.html