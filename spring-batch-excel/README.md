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

Optionally one can also set the `skippedRowsCallback`, `linesToSkip`, `strict` and `rowSetFactory` property.

##### skippedRowsCallback
When rows are skipped an optional `org.springframework.batch.item.excel.RowCallbackHandler` is called with the skipped row. This comes in handy when one needs to write the skipped rows to another file or create some logging.

##### linesToSkip
The number of lines to skip, this applies to each sheet in the Excel file.

##### strict
By default `true`. This controls wether or not an exception is thrown if the file doesn't exists, by default an exception will be thrown.

##### rowSetFactory
For reading rows a `RowSet` abstraction is used. To construct a `RowSet` for the current `Sheet` a `RowSetFactory` is used. The `DefaultRowSetFactory` constructs a `DefaultRowSet` and `DefaultRowSetMetaData`. For construction of the latter a `ColumnNameExtractor` is needed. At the moment there are 2 implementations 

 - `StaticColumnNameExtractor` uses a preset list of column names.
 - `RowNumberColumnNameExtractor` (**the default**) reads a given row (default 0) to determine the column names of the current sheet

### PassThroughRowMapper
Transforms the read row from excel into a `String[]`.

### BeanPropertyRowMapper
Uses a `BeanWrapper` to convert a given row into an object. Uses the column names of the given `RowSet` to map column to properties of the `targetType` or prototype bean.

    <bean id="excelReader" class="org.springframework.batch.item.excel.poi.PoiItemReader">
        <property name="resource" value="/path/to/your/excel/file" />
        <property name="rowMapper">
            <bean class="org.springframework.batch.item.excel.mapping.BeanPropertyRowMapper">
                <property name="targetType" value="com.your.package.Player" />
            <bean>
        </property>
    </bean>
