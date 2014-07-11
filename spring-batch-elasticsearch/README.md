# spring-batch-elasticsearch
--------------
ItemReader and ItemWriter implementations for Elasticsearch

## To index documents
--------------
Create a class and define the index name and type
```
@Document(indexName="some_index", type="some_type")
public class SomeClass {
   // field(s) with getter(s) and setter(s)
}
```

Create an item processor
```
public class SampleItemProcess implements ItemProcessor<Object, IndexQuery> {
 
   @Override
   public IndexQuery process(Object item) throws Exception {
             
      SomeClass someClass = new SomeClass();
      // populate someClass from item (Object)
             
      IndexQueryBuilder builder = new IndexQueryBuilder();
      builder.withObject(someClass);
     // use other methods on builder as required
             
     return builder.build();
  }
}
```

## Configuration for reader/writer
--------------
```
@Configuration
public class ReaderWriterConfig {

	@Bean
	public ElasticsearchItemReader<SomeInputClass> elasticsearchItemReader() {
	     
	    return new ElasticsearchItemReader<>(elasticsearchOperations(), query(), SomeInputClass.class);
	}
	
	@Bean
	public ElasticsearchItemWriter elasticsearchItemWriter() {
	     
	    return new ElasticsearchItemWriter(elasticsearchOperations());
	}
	 
	@Bean
	public SearchQuery query() {
	     
	    NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
	    // create query as required using the methods on the builder object
	     
	    return builder.build();
	}
	 
	@Bean
	public ElasticsearchOperations elasticsearchOperations() {
	    // configure and return elastic search template 
	}
}
```

##### NOTE
The Pageable object from the Query object will be used for paged requests. Setting the page and pageSize fields (inherited from AbstractPaginatedDataItemReader) will have no effect.
