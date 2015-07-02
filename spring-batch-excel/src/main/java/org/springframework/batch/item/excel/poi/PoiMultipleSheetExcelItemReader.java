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

import org.springframework.batch.item.excel.AbstractExcelItemReader;
import org.springframework.batch.item.excel.Sheet;
import org.springframework.core.io.Resource;

/**
 * @author Jyl-Cristoff
 *
 */
public class PoiMultipleSheetExcelItemReader<T> extends AbstractExcelItemReader<T> {

	private PoiItemReader<T> delegate;

	@Override
	protected Sheet getSheet(int sheetNumber) {
		Sheet sheet = delegate.getSheet(sheetNumber);
		if (this.sheetMappings.containsKey(sheetNumber)){
        	Class<? extends T> type = this.sheetMappings.get(sheetNumber);
    		this.rowMapper.setTargetType(type);
    		
        	if (logger.isDebugEnabled()) {
                logger.debug("Openend sheet " + sheet.getName() + ", with target type " + type + " .");
            }
    	}
		return sheet;
	}

	@Override
	public int getNumberOfSheets() {
		return delegate.getNumberOfSheets();
	}

	@Override
	protected void openExcelFile(Resource resource) throws Exception {
		this.currentSheet = 0;
		delegate.openExcelFile(resource);
		
	}

	@Override
	protected void doClose() throws Exception {
		delegate.doClose();
		
	}

	public void setDelegate(PoiItemReader<T> delegate) {
		this.delegate = delegate;
	}

}
