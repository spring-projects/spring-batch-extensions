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

import org.springframework.batch.item.excel.ExcelFileParseException;

/**
 * @author Jyl-Cristoff
 *
 */
public class PoiMultipleSheetExcelItemReader<T> extends
		PoiItemReader<T> {
	
	public T read(int sheetNumber){
		if(this.currentSheet != sheetNumber){
			currentSheet = sheetNumber;
			this.openSheet();
		}
		 if (this.noInput || this.rs == null) {
	            return null;
	        }

	        if (rs.next()) {
	            try {
	                return this.rowMapper.mapRow(rs);
	            } catch (final Exception e) {
	                throw new ExcelFileParseException("Exception parsing Excel file.", e, this.resource.getDescription(),
	                        rs.getMetaData().getSheetName(), rs.getCurrentRowIndex(), rs.getCurrentRow());
	            }
	        } 
	        return null;
	}

}
