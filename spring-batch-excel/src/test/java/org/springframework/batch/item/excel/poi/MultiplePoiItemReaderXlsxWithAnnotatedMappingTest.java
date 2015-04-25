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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.batch.item.Player;
import org.springframework.batch.item.Team;
import org.springframework.batch.item.excel.AbstractExcelItemReader;
import org.springframework.batch.item.excel.AbstractExcelItemReaderTests;
import org.springframework.batch.item.excel.mapping.BeanWrapperRowMapper;
import org.springframework.core.io.ClassPathResource;

public class MultiplePoiItemReaderXlsxWithAnnotatedMappingTest<T> extends AbstractExcelItemReaderTests {

	private Map<Integer, Class<? extends Object>> sheetMappings = new HashMap<Integer, Class<? extends Object>>();
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
    protected void configureItemReader(AbstractExcelItemReader itemReader) {
        itemReader.setResource(new ClassPathResource("org/springframework/batch/item/excel/multiple.xlsx"));
        BeanWrapperRowMapper<Player> rowMapper = new BeanWrapperRowMapper<Player>();
        itemReader.setRowMapper(rowMapper);
        sheetMappings.put(0, Player.class);
        sheetMappings.put(1, Team.class);
        itemReader.setSheetMappings(sheetMappings);
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected AbstractExcelItemReader createExcelItemReader() {
        return new PoiMultipleSheetExcelItemReader();
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    @Override
    public void readExcelFile() throws Exception {
    	
    	T row;
    	assertEquals(3, this.itemReader.getNumberOfSheets());
        for (int i = 0; i < this.itemReader.getNumberOfSheets(); i++) {
        	do {
        		row = (T) ((PoiMultipleSheetExcelItemReader)this.itemReader).read(i);
                this.logger.debug("Read: " + row);
            } while (row != null);
		}
    }
    
}
