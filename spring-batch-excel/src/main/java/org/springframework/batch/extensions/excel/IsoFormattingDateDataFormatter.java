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
import org.apache.poi.ss.usermodel.FormulaEvaluator;

/**
 * Specialized subclass for additionally formatting the date into an ISO date/time.
 *
 * @author Marten Deinum
 *
 * @see DateTimeFormatter#ISO_OFFSET_DATE_TIME
 */
public class IsoFormattingDateDataFormatter extends DataFormatter {

	public IsoFormattingDateDataFormatter() {
		super();
	}

	public IsoFormattingDateDataFormatter(Locale locale) {
		super(locale);
	}

	@Override
	public String formatCellValue(Cell cell, FormulaEvaluator evaluator, ConditionalFormattingEvaluator cfEvaluator) {
		if (cell == null) {
			return "";
		}

		CellType cellType = cell.getCellType();
		if (cellType == CellType.FORMULA) {
			if (evaluator == null) {
				return cell.getCellFormula();
			}
			cellType = evaluator.evaluateFormulaCell(cell);
		}

		if (cellType == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell, cfEvaluator)) {
			LocalDateTime value = cell.getLocalDateTimeCellValue();
			return (value != null) ? value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) : "";
		}
		return super.formatCellValue(cell, evaluator, cfEvaluator);
	}
}
