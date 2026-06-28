/*
 * Copyright 2006-2026 the original author or authors.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.CellValueRecordInterface;
import org.apache.poi.hssf.record.DimensionsRecord;
import org.apache.poi.hssf.record.EOFRecord;
import org.apache.poi.hssf.record.ExtendedFormatRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.MulBlankRecord;
import org.apache.poi.hssf.record.MulRKRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.RKRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RecordFactoryInputStream;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaError;

import org.springframework.batch.extensions.excel.Sheet;
import org.springframework.util.StringUtils;

/**
 * {@code Sheet} implementation for a single worksheet of a legacy binary {@code .xls}
 * (HSSF) workbook, reading the rows lazily from POI's
 * {@link RecordFactoryInputStream} pull stream. It is the HSSF analogue of
 * {@link StreamingSheet}.
 *
 * @author Kebba Manneh
 * @since 0.3.0
 */
class XlsSheet implements Sheet {

	private final Log logger = LogFactory.getLog(XlsSheet.class);

	private final String name;

	private final POIFSFileSystem poifs;

	private final String workbookEntry;

	private final int positionOfBof;

	private final SSTRecord sst;

	private final List<ExtendedFormatRecord> extendedFormats;

	private final Map<Integer, String> formats;

	private final DataFormatter dataFormatter;

	private DocumentInputStream documentStream;

	private RecordFactoryInputStream records;

	private boolean opened;

	private boolean exhausted;

	private int rowCount;

	private int colCount;

	private String[] staged;

	private int currentRow = -1;

	private int pendingFormulaColumn = -1;

	private Record bufferedCell;

	XlsSheet(String name, POIFSFileSystem poifs, String workbookEntry, int positionOfBof, SSTRecord sst,
			List<ExtendedFormatRecord> extendedFormats, Map<Integer, String> formats, DataFormatter dataFormatter) {
		this.name = name;
		this.poifs = poifs;
		this.workbookEntry = workbookEntry;
		this.positionOfBof = positionOfBof;
		this.sst = sst;
		this.extendedFormats = extendedFormats;
		this.formats = formats;
		this.dataFormatter = dataFormatter;
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

	/**
	 * Lazily open an own stream over the workbook, positioned at this sheet's {@code BOF}
	 * record, mirroring the way each {@link StreamingSheet} reads its own
	 * {@code InputStream}.
	 */
	private void ensureOpen() throws IOException {
		if (this.opened) {
			return;
		}
		this.documentStream = this.poifs.createDocumentInputStream(this.workbookEntry);
		skipFully(this.documentStream, this.positionOfBof);
		this.records = new RecordFactoryInputStream(this.documentStream, false);
		this.opened = true;
	}

	private String[] nextRow() {
		try {
			ensureOpen();
			if (this.exhausted && this.bufferedCell == null) {
				return null;
			}

			// Seed the new row from the one-cell look-ahead captured at the previous
			// row boundary, if any.
			if (this.bufferedCell != null) {
				Record cell = this.bufferedCell;
				this.bufferedCell = null;
				startRow(rowOf(cell));
				place(cell);
			}

			Record record;
			while ((record = this.records.nextRecord()) != null) {
				if (record instanceof EOFRecord) {
					this.exhausted = true;
					return flush();
				}
				else if (record instanceof DimensionsRecord dimensions) {
					this.colCount = dimensions.getLastCol();
					this.rowCount = dimensions.getLastRow();
				}
				else if (record instanceof StringRecord stringRecord) {
					// The cached string result of the preceding formula record.
					if (this.pendingFormulaColumn >= 0 && this.staged != null) {
						this.staged[this.pendingFormulaColumn] = stringRecord.getString();
					}
					this.pendingFormulaColumn = -1;
				}
				else {
					int row = rowOf(record);
					if (row < 0) {
						continue;
					}
					if (this.staged != null && row != this.currentRow) {
						// A new row started: hold this cell back and flush the previous row.
						this.bufferedCell = record;
						return flush();
					}
					if (this.staged == null) {
						startRow(row);
					}
					place(record);
				}
			}
			this.exhausted = true;
			return flush();
		}
		catch (IOException ex) {
			throw new IllegalStateException("Error reading file.", ex);
		}
	}

	private void startRow(int row) {
		this.currentRow = row;
		this.pendingFormulaColumn = -1;
		this.staged = new String[Math.max(this.colCount, 0)];
		Arrays.fill(this.staged, "");
	}

	private String[] flush() {
		if (this.staged == null) {
			return null;
		}
		String[] result = Arrays.copyOf(this.staged, this.staged.length);
		this.staged = null;
		this.currentRow = -1;
		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Row ended, returning: " + StringUtils.arrayToCommaDelimitedString(result));
		}
		return result;
	}

	private static int rowOf(Record record) {
		if (record instanceof CellValueRecordInterface cell) {
			return cell.getRow();
		}
		if (record instanceof MulRKRecord mulRk) {
			return mulRk.getRow();
		}
		if (record instanceof MulBlankRecord mulBlank) {
			return mulBlank.getRow();
		}
		return -1;
	}

	private void place(Record record) {
		if (record instanceof MulRKRecord mulRk) {
			ensureCapacity(mulRk.getLastColumn());
			for (int i = 0; i < mulRk.getNumColumns(); i++) {
				this.staged[mulRk.getFirstColumn() + i] = format(mulRk.getRKNumberAt(i), mulRk.getXFAt(i));
			}
			return;
		}
		if (record instanceof MulBlankRecord mulBlank) {
			ensureCapacity(mulBlank.getLastColumn());
			for (int col = mulBlank.getFirstColumn(); col <= mulBlank.getLastColumn(); col++) {
				this.staged[col] = "";
			}
			return;
		}
		CellValueRecordInterface cell = (CellValueRecordInterface) record;
		int col = cell.getColumn();
		ensureCapacity(col);
		this.staged[col] = value(cell);
	}

	private String value(CellValueRecordInterface cell) {
		if (cell instanceof LabelSSTRecord label) {
			return this.sst.getString(label.getSSTIndex()).getString();
		}
		if (cell instanceof NumberRecord number) {
			return format(number.getValue(), number.getXFIndex());
		}
		if (cell instanceof RKRecord rk) {
			return format(rk.getRKNumber(), rk.getXFIndex());
		}
		if (cell instanceof BlankRecord) {
			return "";
		}
		if (cell instanceof BoolErrRecord boolErr) {
			if (boolErr.isBoolean()) {
				return boolErr.getBooleanValue() ? "TRUE" : "FALSE";
			}
			return FormulaError.forInt(boolErr.getErrorValue()).getString();
		}
		if (cell instanceof LabelRecord label) {
			return label.getValue();
		}
		if (cell instanceof FormulaRecord formula) {
			return formulaValue(cell.getColumn(), formula);
		}
		return "";
	}

	private String formulaValue(int column, FormulaRecord formula) {
		if (formula.hasCachedResultString()) {
			// The string result is supplied by the StringRecord that follows; mark the
			// column so it can be filled in when that record arrives.
			this.pendingFormulaColumn = column;
			return "";
		}
		CellType type = formula.getCachedResultTypeEnum();
		if (type == CellType.BOOLEAN) {
			return formula.getCachedBooleanValue() ? "TRUE" : "FALSE";
		}
		if (type == CellType.ERROR) {
			return FormulaError.forInt(formula.getCachedErrorValue()).getString();
		}
		return format(formula.getValue(), formula.getXFIndex());
	}

	// Format a numeric (or date) value using the cell's extended format, reproducing what
	// FormatTrackingHSSFListener does in event mode.
	private String format(double value, int xfIndex) {
		int formatIndex = 0;
		String formatString = null;
		if (xfIndex >= 0 && xfIndex < this.extendedFormats.size()) {
			formatIndex = this.extendedFormats.get(xfIndex).getFormatIndex() & 0xFFFF;
			formatString = this.formats.get(formatIndex);
			if (formatString == null) {
				formatString = BuiltinFormats.getBuiltinFormat(formatIndex);
			}
		}
		if (formatString == null) {
			formatString = BuiltinFormats.getBuiltinFormat(0);
		}
		return this.dataFormatter.formatRawCellContents(value, formatIndex, formatString);
	}

	private void ensureCapacity(int column) {
		if (this.staged == null) {
			this.staged = new String[column + 1];
			Arrays.fill(this.staged, "");
		}
		else if (this.staged.length <= column) {
			int previous = this.staged.length;
			this.staged = Arrays.copyOf(this.staged, column + 1);
			for (int i = previous; i < this.staged.length; i++) {
				this.staged[i] = "";
			}
		}
	}

	private static void skipFully(DocumentInputStream stream, long bytes) throws IOException {
		long remaining = bytes;
		while (remaining > 0) {
			long skipped = stream.skip(remaining);
			if (skipped <= 0) {
				if (stream.read() < 0) {
					throw new IOException("Reached end of stream before the sheet offset.");
				}
				skipped = 1;
			}
			remaining -= skipped;
		}
	}

	@Override
	public void close() throws Exception {
		if (this.documentStream != null) {
			this.documentStream.close();
			this.documentStream = null;
		}
	}

	@Override
	public Iterator<String[]> iterator() {
		return new Iterator<>() {

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

}
