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
import org.springframework.batch.item.Player;
import org.springframework.batch.item.excel.AbstractExcelItemReader;
import org.springframework.batch.item.excel.AbstractExcelItemReaderTests;
import org.springframework.batch.item.excel.mapping.BeanWrapperRowMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

public class PoiItemReaderXlsxWithAnnotatedMappingTest extends AbstractExcelItemReaderTests {

    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    protected void configureItemReader(AbstractExcelItemReader itemReader) {
        itemReader.setResource(new ClassPathResource("org/springframework/batch/item/excel/player_different_headers.xlsx"));
        BeanWrapperRowMapper<Player> rowMapper = new BeanWrapperRowMapper<Player>();
        rowMapper.setTargetType(Player.class);
        itemReader.setRowMapper(rowMapper);
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected AbstractExcelItemReader createExcelItemReader() {
        return new PoiItemReader<Player>();
    }
    
    @Override
    @Test
    public void readExcelFile() throws Exception {
    	Player row;
        do {
            row = (Player) this.itemReader.read();
            this.logger.debug("Read: " + row);
        } while (row != null);
        int readCount = (Integer) ReflectionTestUtils.getField(this.itemReader, "currentItemCount" );
        assertEquals(4359, readCount);
    }
    
}
