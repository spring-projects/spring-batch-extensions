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
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.batch.item.excel.support.rowset.RowSetFactory;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link org.springframework.batch.item.ItemReader} implementation to read an Excel
 * file. It will read the file sheet for sheet and row for row. It is loosy based on
 * the {@link org.springframework.batch.item.file.FlatFileItemReader}
 *
 * @param <T> the type
 * @author Marten Deinum
 * @since 0.5.0
 */
public abstract class AbstractExcelItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements
        ResourceAwareItemReaderItemStream<T>, InitializingBean {

    protected final Log logger = LogFactory.getLog(getClass());
    private Resource resource;
    private int linesToSkip = 0;
    private int currentSheet = 0;
    private int endAfterBlankLines = 1;
    private RowMapper<T> rowMapper;
    private RowCallbackHandler skippedRowsCallback;
    private boolean noInput = false;
    private boolean strict = true;
    private RowSetFactory rowSetFactory = new DefaultRowSetFactory();
    private RowSet rs;

    public AbstractExcelItemReader() {
        super();
        this.setName(ClassUtils.getShortName(this.getClass()));
    }
    
    @Override
	public T read() throws Exception, UnexpectedInputException, ParseException {
        	T item = super.read();
        	int blankLines = 0;
        	while (item == null) {
        		blankLines++;
        		if (blankLines >= endAfterBlankLines) {
        			return null;
        		}
        		item = super.read();
        		if (item != null) {
        			return item;
        		}
        	}
        	return item;
	}

    /**
     * @return string corresponding to logical record according to
     * {@link #setRowMapper(RowMapper)} (might span multiple rows in file).
     */
    @Override
    protected T doRead() throws Exception {
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
        } else {
            this.currentSheet++;
            if (this.currentSheet >= this.getNumberOfSheets()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("No more sheets in '" + this.resource.getDescription() + "'.");
                }
                return null;
            } else {
                this.openSheet();
                return this.doRead();
            }
        }
    }
    
    /**
	 * On restart this will increment rowSet to where job left off previously
	 */
	@Override
	protected void jumpToItem(final int itemIndex) throws Exception {
		for (int i = 0; i < itemIndex; i++) {
			rs.next();
		}
	}

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        this.noInput = true;
        if (!this.resource.exists()) {
            if (this.strict) {
                throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): "
                        + this.resource);
            }
            logger.warn("Input resource does not exist '" + this.resource.getDescription() + "'.");
            return;
        }

        if (!this.resource.isReadable()) {
            if (this.strict) {
                throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): "
                        + this.resource);
            }
            logger.warn("Input resource is not readable '" + this.resource.getDescription() + "'.");
            return;
        }

        this.openExcelFile(this.resource);
        this.openSheet();
        this.noInput = false;
        if (logger.isDebugEnabled()) {
            logger.debug("Opened workbook [" + this.resource.getFilename() + "] with " + this.getNumberOfSheets() + " sheets.");
        }
    }

    private void openSheet() {
        final Sheet sheet = this.getSheet(this.currentSheet);
        this.rs =rowSetFactory.create(sheet);


        if (logger.isDebugEnabled()) {
            logger.debug("Opening sheet " + sheet.getName() + ".");
        }

        for (int i = 0; i < this.linesToSkip; i++) {
            if (rs.next() && this.skippedRowsCallback != null) {
                this.skippedRowsCallback.handleRow(rs);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Openend sheet " + sheet.getName() + ", with " + sheet.getNumberOfRows() + " rows.");
        }
    }

    protected void doClose() throws Exception {
        this.currentSheet=0;
        this.rs=null;
    }

        /**
         * Public setter for the input resource.
         *
         * @param resource the {@code Resource} pointing to the Excelfile
         */
    public void setResource(final Resource resource) {
        this.resource = resource;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(this.rowMapper, "RowMapper must be set");
    }

    /**
     * Set the number of lines to skip. This number is applied to all worksheet
     * in the excel file! default to 0
     *
     * @param linesToSkip number of lines to skip
     */
    public void setLinesToSkip(final int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    /**
     *
     * @param sheet the sheet index
     * @return the sheet or <code>null</code> when no sheet available.
     */
    protected abstract Sheet getSheet(int sheet);

    /**
     * The number of sheets in the underlying workbook.
     *
     * @return the number of sheets.
     */
    protected abstract int getNumberOfSheets();

    /**
     *
     * @param resource {@code Resource} pointing to the Excel file to read
     * @throws Exception when the Excel sheet cannot be accessed
     */
    protected abstract void openExcelFile(Resource resource) throws Exception;

    /**
     * In strict mode the reader will throw an exception on
     * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input resource does not exist.
     *
     * @param strict true by default
     */
    public void setStrict(final boolean strict) {
        this.strict = strict;
    }

    /**
     * Public setter for the {@code rowMapper}. Used to map a row read from the underlying Excel workbook.
     *
     * @param rowMapper the {@code RowMapper} to use.
     */
    public void setRowMapper(final RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    /**
     * Public setter for the <code>rowSetFactory</code>. Used to create a {@code RowSet} implemenation. By default the
     * {@code DefaultRowSetFactory} is used.
     *
     * @param rowSetFactory the {@code RowSetFactory} to use.
     */
    public void setRowSetFactory(RowSetFactory rowSetFactory) {
        this.rowSetFactory = rowSetFactory;
    }

    /**
     * @param skippedRowsCallback will be called for each one of the initial skipped lines before any items are read.
     */
    public void setSkippedRowsCallback(final RowCallbackHandler skippedRowsCallback) {
        this.skippedRowsCallback = skippedRowsCallback;
    }
    
    public void setEndAfterBlankLines(final int endAfterBlankLines) {
		this.endAfterBlankLines = endAfterBlankLines;
	}
}
