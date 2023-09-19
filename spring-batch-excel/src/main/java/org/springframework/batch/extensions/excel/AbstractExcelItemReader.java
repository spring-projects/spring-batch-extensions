/*
 * Copyright 2006-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.extensions.excel;

import java.util.Locale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.DataFormatter;

import org.springframework.batch.extensions.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.extensions.excel.support.rowset.RowSet;
import org.springframework.batch.extensions.excel.support.rowset.RowSetFactory;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link org.springframework.batch.item.ItemReader} implementation to read an Excel file.
 * It will read the file sheet for sheet and row for row. It is loosy based on the
 * {@link org.springframework.batch.item.file.FlatFileItemReader}
 *
 * @param <T> the type
 * @author Marten Deinum
 * @since 0.1.0
 */
public abstract class AbstractExcelItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements ResourceAwareItemReaderItemStream<T>, InitializingBean {

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

	private String password;

	private boolean datesAsIso = false;

	private Locale userLocale;

	private DataFormatter dataFormatter;

	public AbstractExcelItemReader() {
		super();
		this.setName(ClassUtils.getShortName(this.getClass()));
	}

	@Override
	public T read() throws Exception {
		T item = super.read();
		int blankLines = 0;
		while (item == null) {
			blankLines++;
			if (blankLines >= this.endAfterBlankLines) {
				return null;
			}
			item = super.read();
			if (item != null) {
				return item;
			}
		}
		return item;
	}

	@Override
	protected T doRead() {
		if (this.noInput) {
			return null;
		}

		if (this.rs == null || !this.rs.next()) {
			if (!nextSheet()) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("No more sheets in '" + this.resource.getDescription() + "'.");
				}
				return null;
			}
		}

		// skip all the blank row from which content has been deleted but still a valid row
		while (null != this.rs.getCurrentRow() && isInvalidValidRow(this.rs)) {
			this.rs.next();
		}
		try {
			return (this.rs.getCurrentRow() != null) ? this.rowMapper.mapRow(this.rs) : doRead();
		}
		catch (Exception ex) {
			throw new ExcelFileParseException("Exception parsing Excel file.", ex, this.resource.getDescription(),
					this.rs.getMetaData().getSheetName(), this.rs.getCurrentRowIndex(), this.rs.getCurrentRow());
		}
	}

	/**
	 * On restart this will increment rowSet to where job left off previously.
	 * Temporarily switch out the configured {@code RowMapper} so we can use the
	 * {@code #doRead} method and reuse the logic in there, but without actually map to
	 * instances (this to save memory and have better performance).
	 */
	@Override
	protected void jumpToItem(final int itemIndex) {
		RowMapper<T> current = this.rowMapper;
		this.rowMapper = (rs) -> null;
		try {
			for (int i = 0; i < itemIndex; i++) {
				doRead();
			}
		}
		finally {
			this.rowMapper = current;
		}
	}

	private boolean isInvalidValidRow(RowSet rs) {
		for (String str : rs.getCurrentRow()) {
			if (str.length() > 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.notNull(this.resource, "Input resource must be set");
		this.noInput = true;
		if (!this.resource.exists()) {
			if (this.strict) {
				throw new IllegalStateException(
						"Input resource must exist (reader is in 'strict' mode): " + this.resource);
			}
			this.logger.warn("Input resource does not exist '" + this.resource.getDescription() + "'.");
			return;
		}

		if (!this.resource.isReadable()) {
			if (this.strict) {
				throw new IllegalStateException(
						"Input resource must be readable (reader is in 'strict' mode): " + this.resource);
			}
			this.logger.warn("Input resource is not readable '" + this.resource.getDescription() + "'.");
			return;
		}

		this.openExcelFile(this.resource, this.password);
		this.noInput = false;
		if (this.logger.isDebugEnabled()) {
			this.logger.debug("Opened workbook [" + this.resource.getFilename() + "] with " + this.getNumberOfSheets()
					+ " sheets.");
		}
	}

	private boolean nextSheet() {
		while (this.currentSheet < this.getNumberOfSheets()) {
			final Sheet sheet = this.getSheet(this.currentSheet);
			this.rs = this.rowSetFactory.create(sheet);
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Opening sheet " + sheet.getName() + ".");
			}

			for (int i = 0; i < this.linesToSkip; i++) {
				if (this.rs.next() && this.skippedRowsCallback != null) {
					this.skippedRowsCallback.handleRow(this.rs);
				}
			}
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Openend sheet " + sheet.getName() + ", with " + sheet.getNumberOfRows() + " rows.");
			}
			this.currentSheet++;
			if (this.rs.next()) {
				return true;
			}
		}
		return false;
	}

	protected void doClose() throws Exception {
		this.currentSheet = 0;
		this.rs = null;
	}

	/**
	 * Public setter for the input resource.
	 * @param resource the {@code Resource} pointing to the Excelfile
	 */
	public void setResource(final Resource resource) {
		this.resource = resource;
	}

	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.rowMapper, "RowMapper must be set");
		if (this.datesAsIso) {
			this.dataFormatter = (this.userLocale != null) ? new IsoFormattingDateDataFormatter(this.userLocale) : new IsoFormattingDateDataFormatter();
		}
		else {
			this.dataFormatter = (this.userLocale != null) ? new DataFormatter(this.userLocale) : new DataFormatter();
		}
	}

	protected DataFormatter getDataFormatter() {
		return this.dataFormatter;
	}

	/**
	 * Set the number of lines to skip. This number is applied to all worksheet in the
	 * excel file! default to 0
	 * @param linesToSkip number of lines to skip
	 */
	public void setLinesToSkip(final int linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	/**
	 * Get the sheet based on the given sheet index.
	 * @param sheet the sheet index
	 * @return the sheet or <code>null</code> when no sheet available.
	 */
	protected abstract Sheet getSheet(int sheet);

	/**
	 * The number of sheets in the underlying workbook.
	 * @return the number of sheets.
	 */
	protected abstract int getNumberOfSheets();

	/**
	 * Opens the excel file and reads the file and sheet metadata. Uses a {@code Resource} to read the sheets,
	 * this file can optionally be password protected.
	 * @param resource {@code Resource} pointing to the Excel file to read
	 * @param password optional password
	 * @throws Exception when the Excel sheet cannot be accessed
	 */
	protected abstract void openExcelFile(Resource resource, String password) throws Exception;

	/**
	 * In strict mode the reader will throw an exception on
	 * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input
	 * resource does not exist.
	 * @param strict true by default
	 */
	public void setStrict(final boolean strict) {
		this.strict = strict;
	}

	/**
	 * Public setter for the {@code rowMapper}. Used to map a row read from the underlying
	 * Excel workbook.
	 * @param rowMapper the {@code RowMapper} to use.
	 */
	public void setRowMapper(final RowMapper<T> rowMapper) {
		this.rowMapper = rowMapper;
	}

	/**
	 * Public setter for the <code>rowSetFactory</code>. Used to create a {@code RowSet}
	 * implemenation. By default the {@code DefaultRowSetFactory} is used.
	 * @param rowSetFactory the {@code RowSetFactory} to use.
	 */
	public void setRowSetFactory(RowSetFactory rowSetFactory) {
		this.rowSetFactory = rowSetFactory;
	}

	/**
	 * Set the callback handler to call when a row is being skipped.
	 * @param skippedRowsCallback will be called for each one of the initial skipped lines
	 * before any items are read.
	 */
	public void setSkippedRowsCallback(final RowCallbackHandler skippedRowsCallback) {
		this.skippedRowsCallback = skippedRowsCallback;
	}

	public void setEndAfterBlankLines(final int endAfterBlankLines) {
		this.endAfterBlankLines = endAfterBlankLines;
	}

	/**
	 * The password used to protect the file to open.
	 * @param password the password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Instead of using the format defined in the Excel sheet, read the date/time fields as an ISO formatted
	 * string instead. This is by default {@code false} to leave the original behavior.
	 * @param datesAsIso default {@code false}
	 */
	public void setDatesAsIso(boolean datesAsIso) {
		this.datesAsIso = datesAsIso;
	}

	/**
	 * The {@code Locale} to use when reading sheets. Defaults to the platform default as set by Java.
	 * @param userLocale the {@code Locale} to use, default {@code null}
	 */
	public void setUserLocale(Locale userLocale) {
		this.userLocale = userLocale;
	}
}
