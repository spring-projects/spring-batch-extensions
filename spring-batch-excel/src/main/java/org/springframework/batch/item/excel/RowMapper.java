/*
 * Copyright 2006-2015 the original author or authors.
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

import org.springframework.batch.item.excel.support.rowset.RowSet;

/**
 * Map rows from an excel sheet to an object.
 *
 * @param <T> the type
 * @author Marten Deinum
 * @since 0.5.0
 */
public interface RowMapper<T> {

    /**
     * Implementations must implement this method to map the provided row to
     * the parameter type T.  The row number represents the number of rows
     * into a {@link Sheet} the current line resides.
     *
     * @param rs the RowSet used for mapping.
     * @return mapped object of type T
     * @throws Exception if error occured while parsing.
     */
    T mapRow(RowSet rs) throws Exception;
    
    /**
     * Public setter for the type of bean to create instead of using a prototype
     * bean. An object of this type will be created from its default constructor
     * for every call to {@link #mapRow(org.springframework.batch.item.excel.support.rowset.RowSet)}.<br>
     *
     * Either this property or the prototype bean name must be specified, but
     * not both.
     *
     * @param type the type to set
     */
     void setTargetType(Class<? extends T> type);

}
