/*
 * Copyright 2006-2017 the original author or authors.
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
package org.springframework.batch.item.excel.mapping;

import org.springframework.batch.item.excel.RowMapper;
import org.springframework.batch.item.excel.support.rowset.RowSet;

/**
 * Pass through {@link RowMapper} useful for passing the original row
 * back directly rather than a mapped object.
 *
 * @param <R> Type used for representing a single row, such as an array
 * @author Marten Deinum
 * @since 0.5.0
 */
public class PassThroughRowMapper<R> implements RowMapper<R, R> {

    @Override
    public R mapRow(final RowSet<R> rs) throws Exception {
        return rs.getCurrentRow();
    }

}
