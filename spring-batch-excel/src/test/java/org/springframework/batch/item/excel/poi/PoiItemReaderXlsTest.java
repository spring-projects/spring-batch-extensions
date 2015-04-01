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
package org.springframework.batch.item.excel.poi;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.springframework.batch.item.excel.AbstractExcelItemReader;
import org.springframework.batch.item.excel.AbstractExcelItemReaderTests;
import org.springframework.util.StringUtils;

public class PoiItemReaderXlsTest extends AbstractExcelItemReaderTests {

    @Override
    protected AbstractExcelItemReader createExcelItemReader() {
        return new PoiItemReader();
    }
    
    @Test
    public void readNumberOfColumnsFromHeaderRow() throws Exception {
    	int headerRowNumber = -1;
    	    		
    	String[] row;
        do {
            row = (String[]) this.itemReader.read();
            this.logger.debug("Read: " + StringUtils.arrayToCommaDelimitedString(row));
            if (row != null) {
                assertEquals(6, row.length);
            }
        } while (row != null);
    }   
}