/*
 * Copyright 2011-2022 the original author or authors.
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.apache.poi.ss.formula.ConditionalFormattingEvaluator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.ExcelNumberFormat;
import org.apache.poi.ss.usermodel.FormulaEvaluator;

/**
 * Specialized subclass for formatting the date into an ISO date/time and ignore the format as given in the Excel file.
 *
 * @author Marten Deinum
 */
public class IsoFormattingDateDataFormatter extends DataFormatter {

	public IsoFormattingDateDataFormatter() {
		super();
	}

	public IsoFormattingDateDataFormatter(Locale locale) {
		super(locale);
	}

	@Override
	public String formatRawCellContents(double value, int formatIndex, String formatString, boolean use1904Windowing) {

		if (DateUtil.isADateFormat(formatIndex, formatString) && DateUtil.isValidExcelDate(value)) {
			String formatToUse = determineFormat(formatIndex);
			return super.formatRawCellContents(value, formatIndex, formatToUse, use1904Windowing);
		}
		return super.formatRawCellContents(value, formatIndex, formatString, use1904Windowing);
	}

	@Override
	public String formatCellValue(Cell cell, FormulaEvaluator evaluator, ConditionalFormattingEvaluator cfEvaluator) {
		if (cell == null) {
			return "";
		}

		CellType cellType = cell.getCellType();
		if (cellType == CellType.FORMULA && useCachedValuesForFormulaCells()) {
			cellType = cell.getCachedFormulaResultType();
		}

		if (cellType != CellType.STRING && DateUtil.isCellDateFormatted(cell, cfEvaluator)) {
			String formatToUse = determineFormat(ExcelNumberFormat.from(cell, cfEvaluator).getIdx());
			LocalDateTime value = cell.getLocalDateTimeCellValue();
			return (value != null) ? value.format(DateTimeFormatter.ofPattern(formatToUse)) : "";
		}
		return super.formatCellValue(cell, evaluator, cfEvaluator);
	}

	/**
	 * Determine the format to use for either date, time of datetime. Based on the internal formats used by Excel.
	 * 14, 15, 16, 17 are dates only
	 * 18, 19, 20, 21 are times only
	 * anything else is interpreted as a datetime, including custom formats that might be in use!
	 * @param formatIndex the format index from excel.
	 * @return the format to use, never {@code null}.
	 */

	private String determineFormat(int formatIndex) {
		if (formatIndex >= 14 && formatIndex < 18) {
			return "yyyy-MM-dd";
		}
		else if (formatIndex >= 18 && formatIndex < 22) {
			return "HH:mm:ss";
		}
		return "yyyy-MM-dd'T'HH:mm:ss";
	}
}
