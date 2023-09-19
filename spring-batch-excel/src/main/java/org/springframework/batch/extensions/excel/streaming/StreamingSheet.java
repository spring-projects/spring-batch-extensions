/*
 * Copyright 2006-2021 the original author or authors.
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

package org.springframework.batch.extensions.excel.streaming;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.Styles;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.Attributes;

import org.springframework.batch.extensions.excel.Sheet;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.StaxUtils;

/**
 * {@code Sheet} implementation for Apache POI using the streaming event mode to read the rows.
 *
 * @author Marten Deinum
 * @since 0.1.0
 */
class StreamingSheet implements Sheet {

	private final Log logger = LogFactory.getLog(StreamingSheet.class);

	private final String name;

	private final InputStream is;

	private final XMLStreamReader reader;

	private final ValueRetrievingContentsHandler contentHandler;

	private final XSSFSheetXMLHandler sheetHandler;

	private int rowCount;

	private int colCount;

	StreamingSheet(String name, InputStream is, SharedStrings sharedStrings, Styles styles, DataFormatter dataFormatter) {
		this.name = name;
		this.is = is;
		this.contentHandler = new ValueRetrievingContentsHandler();
		this.sheetHandler = new XSSFSheetXMLHandler(styles, sharedStrings, this.contentHandler, dataFormatter, false);

		try {
			this.reader = StaxUtils.createDefensiveInputFactory().createXMLStreamReader(is);
		}
		catch (Exception ex) {
			throw new IllegalStateException(ex);
		}
	}

	@Override
	public int getNumberOfRows() {
		return this.rowCount;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public String[] getRow(int rowNumber) {
		throw new UnsupportedOperationException("Getting row by index not supported when streaming.");
	}

	private String[] nextRow() {
		try {
			while (this.reader.hasNext()) {
				int type = this.reader.next();
				if (type == XMLStreamConstants.START_DOCUMENT) {
					this.sheetHandler.startDocument();
				}
				else if (type == XMLStreamConstants.END_DOCUMENT) {
					this.sheetHandler.endDocument();
					return null;
				}
				else if (type == XMLStreamConstants.CHARACTERS) {
					this.sheetHandler.characters(this.reader.getTextCharacters(), this.reader.getTextStart(), this.reader.getTextLength());
				}
				else if (type == XMLStreamConstants.START_ELEMENT) {
					String localName = this.reader.getLocalName();
					if ("dimension".equals(localName)) {
						String v = this.reader.getAttributeValue(null, "ref");
						if (v != null && v.indexOf(':') > -1) {
							CellRangeAddress range = CellRangeAddress.valueOf(v);
							int rowEnd = range.getLastRow();
							int rowStart = range.getFirstRow();
							this.rowCount = rowEnd - rowStart + 1;

							int colStart = range.getFirstColumn();
							int colEnd = range.getLastColumn();
							this.colCount = colEnd - colStart + 1;
						}
					}
					else {
						Attributes delegating = new AttributesAdapter(this.reader);
						this.sheetHandler.startElement(null, localName, null, delegating);
					}
				}
				else if (type == XMLStreamConstants.END_ELEMENT) {
					String tag = this.reader.getLocalName();
					this.sheetHandler.endElement(null, tag, null);
					if ("row".equals(tag)) {
						if (this.logger.isTraceEnabled()) {
							this.logger.trace("Row ended, returning: "
									+ StringUtils.arrayToCommaDelimitedString(this.contentHandler.getValues()));
						}
						return this.contentHandler.getValues();
					}
				}
			}
		}
		catch (Exception ex) {
			throw new IllegalStateException("Error reading file.", ex);
		}
		return null;
	}

	@Override
	public void close() throws Exception {
		try {
			this.reader.close();
		}
		catch (XMLStreamException ex) {
			// Ignore exception we cannot recover
		}

		this.is.close();
	}

	@Override
	public Iterator<String[]> iterator() {
		return new Iterator<String[]>() {

			private String[] currentRow;

			@Override
			public boolean hasNext() {
				this.currentRow = nextRow();
				return this.currentRow != null;
			}

			@Override
			public String[] next() {
				return this.currentRow;
			}
		};
	}

	private class ValueRetrievingContentsHandler implements XSSFSheetXMLHandler.SheetContentsHandler {

		private final Log logger = LogFactory.getLog(ValueRetrievingContentsHandler.class);

		private String[] values;

		@Override
		public void startRow(int rowNum) {
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("Start processing row: " + rowNum);
			}
			// Prepare for this row
			if (this.values == null) {
				this.values = new String[StreamingSheet.this.colCount];
			}
			Arrays.fill(this.values, "");
		}

		@Override
		public void endRow(int rowNum) {
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("End processing row: " + rowNum);
			}
		}

		@Override
		public void cell(String cellReference, String formattedValue, XSSFComment comment) {
			int col = new CellReference(cellReference).getCol();
			if (this.logger.isTraceEnabled()) {
				this.logger.trace("Setting value (" + cellReference + ") = " + formattedValue);
			}
			// This can happen if the dimensions cannot be read properly but there are
			// still rows.
			// Create a copy of the existing array and append to it.
			if (this.values.length <= col) {
				String[] newValues = Arrays.copyOf(this.values, col + 1);
				Arrays.setAll(newValues, (idx) -> (newValues[idx] != null) ? newValues[idx] : "");
				this.values = newValues;
			}
			this.values[col] = formattedValue;
		}

		String[] getValues() {
			return Arrays.copyOf(this.values, this.values.length);
		}

	}

	/**
	 * Minimal adapter for {@code Attributes} so that it works with the
	 * {@code XSSFSheetXMLHandler}. Adapts an {@code XMLStreamReader} so that it can be
	 * used as an {@code org.xml.sax.Attributes} implementation.
	 */
	private static final class AttributesAdapter implements Attributes {

		private final Map<String, String> attributes = new HashMap<>();

		private AttributesAdapter(XMLStreamReader delegate) {
			for (int i = 0; i < delegate.getAttributeCount(); i++) {
				String name = delegate.getAttributeLocalName(i);
				String value = delegate.getAttributeValue(i);
				this.attributes.put(name, value);
			}
		}

		@Override
		public int getLength() {
			return this.attributes.size();
		}

		@Override
		public String getURI(int index) {
			return null;
		}

		@Override
		public String getLocalName(int index) {
			return null;
		}

		@Override
		public String getQName(int index) {
			return null;
		}

		@Override
		public String getType(int index) {
			return null;
		}

		@Override
		public String getValue(int index) {
			return null;
		}

		@Override
		public int getIndex(String uri, String localName) {
			return 0;
		}

		@Override
		public int getIndex(String qName) {
			return 0;
		}

		@Override
		public String getType(String uri, String localName) {
			return null;
		}

		@Override
		public String getType(String qName) {
			return null;
		}

		@Override
		public String getValue(String uri, String localName) {
			return this.attributes.get(localName);
		}

		@Override
		public String getValue(String qName) {
			return this.attributes.get(qName);
		}

	}

}
