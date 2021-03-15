/*
 * Copyright 2011 the original author or authors.
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

import org.springframework.batch.extensions.excel.support.rowset.RowSet;

/**
 * Callback to handle skipped lines. Useful for header/footer processing.
 *
 * @author Marten Deinum
 */
public interface RowCallbackHandler {

	/**
	 * Implementations must implement this method to process each row of data in the
	 * {@code RowSet}.
	 * <p>This method should not call {@code next()} on the {@code RowSetSet}; it is only
	 * supposed to extract values of the current row.
	 * <p>Exactly what the implementation chooses to do is up to it: A trivial implementation
	 * might simply count rows, while another implementation might build a special header
	 * row.
	 * @param rs the {@code RowSet} to process (preset at the current row)
	 */
	void handleRow(RowSet rs);

}
