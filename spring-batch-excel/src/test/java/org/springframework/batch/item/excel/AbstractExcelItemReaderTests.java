/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.excel.mapping.PassThroughRowMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.StringUtils;

import static org.junit.Assert.assertEquals;

/**
 * Base class for testing Excel based item readers.
 *
 * @author Marten Deinum
 */
public abstract class AbstractExcelItemReaderTests  {

    private final Log logger = LogFactory.getLog(this.getClass());

    protected AbstractExcelItemReader itemReader;

    private ExecutionContext executionContext;

    @Before
    public void setup() throws Exception {
        this.itemReader = createExcelItemReader();
        this.itemReader.setLinesToSkip(1); //First line is column names
        this.itemReader.setResource(new ClassPathResource("org/springframework/batch/item/excel/player.xls"));
        this.itemReader.setRowMapper(new PassThroughRowMapper());
        this.itemReader.setSkippedRowsCallback(new RowCallbackHandler() {

            public void handleRow(final Sheet sheet, final String[] row) {
                logger.info("Skipping: " + StringUtils.arrayToCommaDelimitedString(row));
            }
        });
        configureItemReader(this.itemReader);
        this.itemReader.afterPropertiesSet();
        executionContext = new ExecutionContext();
        this.itemReader.open(executionContext);
    }

    protected void configureItemReader(AbstractExcelItemReader itemReader) {
    }

    @After
    public void after() throws Exception {
        this.itemReader.close();
    }

    @Test
    public void readExcelFile() throws Exception {
        assertEquals(3, this.itemReader.getNumberOfSheets());
        String[] row = null;
        do {
            row = (String[]) this.itemReader.read();
            this.logger.debug("Read: " + StringUtils.arrayToCommaDelimitedString(row));
            if (row != null) {
                assertEquals(6, row.length);
            }
        } while (row != null);
        int readCount = (Integer) ReflectionTestUtils.getField(this.itemReader, "currentItemCount" );
        assertEquals(4320, readCount); // File contains 4321 lines, first is header 4321-1=4320 records read.
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRequiredProperties() throws Exception {
        final AbstractExcelItemReader reader = createExcelItemReader();
        reader.afterPropertiesSet();
    }

    protected abstract AbstractExcelItemReader createExcelItemReader();

}
