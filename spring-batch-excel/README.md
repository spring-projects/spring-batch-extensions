# spring-batch-excel

Spring Batch extension which contains ItemReader implementations for Excel. Support for both JExcel and Apache POI is available.

## Configuration

Next to the [configuration of Spring Batch](http://docs.spring.io/spring-batch/2.2.x/reference/html/configureJob.html) one needs to configure an `ItemReader` which knows how to read an Excel file. 
There are 2 `ItemReaders` one can configure:

- `org.springframework.batch.item.excel.jxl.JxlItemReader`
- `org.springframework.batch.item.excel.poi.PoiItemReader`

Configuration of both readers is the same, the difference is the used technology ([JExcel](http://jexcelapi.sourceforge.net) or [Apache Poi](http://poi.apache.org)) and cababilities of both technologies (JExcel cannot read xlsx files, whereas Apache POI can read those).

    <bean id="excelReader" class="org.springframework.batch.item.excel.poi.PoiItemReader">
        <property name="resource" value="/path/to/your/excel/file" />
        <property name="rowMapper">
            <bean class="org.springframework.batch.item.excel.mapping.PassThroughRowMapper" />
        </property>
    </bean>

Each reader takes a `resource` which is the excel file to read and a `rowMapper` which transforms the row in excel to an object which you can use in the rest of the process. 
The project has 2 default `org.springframework.batch.item.excel.RowMapper` implementations.

Optionally one can also set the `skippedRowsCallback`, `linesToSkip` and `strict` property.

##### skippedRowsCallback
When rows are skipped an optional `org.springframework.batch.item.excel.RowCallbackHandler` is called with the skipped row. This comes in handy when one needs to write the skipped rows to another file or create some logging.

##### linesToSkip
The number of lines to skip, this applies to each sheet in the Excel file.

##### strict
By default `true`. This controls wether or not an exception is thrown if the file doesn't exists, by default an exception will be thrown.

### PassThroughRowMapper
Transforms the read row from excel into a `String[]`.

### DefaultRowMapper
Tries to convert the given row into an object, for this a `org.springframework.batch.item.excel.transform.RowTokenizer` and a `org.springframework.batch.item.file.mapping.FieldSetMapper` (from [the Spring Batch project](http://docs.spring.io/spring-batch/2.2.x/reference/html/readersAndWriters.html#fieldSetMapper)) is needed.

By default the `org.springframework.batch.item.excel.transform.DefaultRowTokenizer` is used. This implementation assumes that the first row contains the column names. The column names are used to map to attributes on the object we are trying to create. How this mapping is done is configurable through a `org.springframework.batch.item.excel.transform.ColumnToAttributeConverter` implementation. 
Again 2 implementations are provided by the framework

- `org.springframework.batch.item.excel.transform.PassThroughColumnToAttributeConverter`
- `org.springframework.batch.item.excel.transform.MappingColumnToAttributeConverter`

The `PassThroughColumnToAttributeConverter` simply assumes that the column name corresponds to a property on the object that is being created. 
The `MappingColumnToAttributeConverter` contains a (optional) column-name to property-name mapping.
