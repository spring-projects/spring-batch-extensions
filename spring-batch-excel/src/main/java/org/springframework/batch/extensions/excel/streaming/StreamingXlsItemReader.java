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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.EOFRecord;
import org.apache.poi.hssf.record.ExtendedFormatRecord;
import org.apache.poi.hssf.record.FormatRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RecordFactoryInputStream;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import org.springframework.batch.extensions.excel.AbstractExcelItemReader;
import org.springframework.batch.extensions.excel.Sheet;
import org.springframework.core.io.Resource;

/**
 * Low-memory streaming {@code ItemReader} for legacy binary {@code .xls} (BIFF8 / HSSF)
 * workbooks. It mirrors {@link StreamingXlsxItemReader} but reads the OLE2
 * {@code "Workbook"} stream record-by-record using Apache POI's pull primitive
 * {@link RecordFactoryInputStream}, so rows are produced lazily with a bounded memory
 * footprint instead of materialising the whole workbook object graph like the
 * {@code PoiItemReader}.
 * <p>
 * The shared, immutable workbook globals (the {@link SSTRecord shared-string table} and
 * the number/date format table) are read once when the file is opened and handed,
 * read-only, to every {@link XlsSheet}. Each sheet then opens its own stream positioned
 * at its {@code BOF} offset, matching the way {@code StreamingXlsxItemReader} gives every
 * sheet its own {@code InputStream}.
 * <p>
 * Encrypted {@code .xls} files are not supported by this reader (consistent with
 * {@code StreamingXlsxItemReader}); a configured password is ignored.
 *
 * @param <T> the type
 * @author Kebba Manneh
 * @since 0.3.0
 */
public class StreamingXlsItemReader<T> extends AbstractExcelItemReader<T> {

	/**
	 * Candidate names of the workbook stream inside the OLE2 container, newest first.
	 */
	private static final String[] WORKBOOK_ENTRY_NAMES = {"Workbook", "Book", "WORKBOOK", "BOOK", "workbook", "book"};

	private final List<XlsSheet> sheets = new ArrayList<>();

	private POIFSFileSystem poifs;

	@Override
	protected Sheet getSheet(int sheet) {
		return this.sheets.get(sheet);
	}

	@Override
	protected int getNumberOfSheets() {
		return this.sheets.size();
	}

	@Override
	protected void openExcelFile(Resource resource, String password) throws Exception {
		this.poifs = resource.isFile() ? new POIFSFileSystem(resource.getFile())
				: new POIFSFileSystem(resource.getInputStream());
		String workbookEntry = resolveWorkbookEntry(this.poifs);
		initSheets(workbookEntry);
	}

	// Read the globals substream once to capture the shared, immutable state (shared
	// strings + format table + the bound sheets), then create one lazy XlsSheet per
	// worksheet sharing that state.
	private void initSheets(String workbookEntry) throws IOException {
		SSTRecord sst = null;
		List<ExtendedFormatRecord> extendedFormats = new ArrayList<>();
		Map<Integer, String> formats = new HashMap<>();
		List<BoundSheetRecord> boundSheets = new ArrayList<>();

		try (DocumentInputStream dis = this.poifs.createDocumentInputStream(workbookEntry)) {
			RecordFactoryInputStream rfis = new RecordFactoryInputStream(dis, false);
			Record record;
			while ((record = rfis.nextRecord()) != null) {
				if (record instanceof EOFRecord) {
					// End of the globals substream, everything shared has been read.
					break;
				}
				else if (record instanceof SSTRecord sstRecord) {
					sst = sstRecord;
				}
				else if (record instanceof ExtendedFormatRecord extendedFormat) {
					extendedFormats.add(extendedFormat);
				}
				else if (record instanceof FormatRecord format) {
					formats.put(format.getIndexCode(), format.getFormatString());
				}
				else if (record instanceof BoundSheetRecord boundSheet) {
					boundSheets.add(boundSheet);
				}
			}
		}

		for (BoundSheetRecord boundSheet : BoundSheetRecord.orderByBofPosition(boundSheets)) {
			this.sheets.add(new XlsSheet(boundSheet.getSheetname(), this.poifs, workbookEntry,
					boundSheet.getPositionOfBof(), sst, extendedFormats, formats, getDataFormatter()));
		}

		if (this.logger.isTraceEnabled()) {
			this.logger.trace("Prepared " + this.sheets.size() + " sheets.");
		}
	}

	private static String resolveWorkbookEntry(POIFSFileSystem poifs) {
		for (String name : WORKBOOK_ENTRY_NAMES) {
			if (poifs.getRoot().hasEntry(name)) {
				return name;
			}
		}
		throw new IllegalStateException("Unable to locate a workbook stream in the OLE2 container.");
	}

	@Override
	protected void doClose() throws Exception {
		for (XlsSheet sheet : this.sheets) {
			sheet.close();
		}
		this.sheets.clear();

		if (this.poifs != null) {
			this.poifs.close();
			this.poifs = null;
		}
		super.doClose();
	}

}
