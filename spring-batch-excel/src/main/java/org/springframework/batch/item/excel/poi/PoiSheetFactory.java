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

package org.springframework.batch.item.excel.poi;

/**
 * Interface for factory that will instantiate {@link PoiSheet}s from Apache POI {@link org.apache.poi.ss.usermodel.Sheet} 
 *
 * @param <R> Type used for representing a single row, such as an array
 * @author Mattias Jiderhamn
 * @since 0.5.0
 */
public interface PoiSheetFactory<R> {
    
    /** Create new {@link PoiSheet} instance */
    PoiSheet<R> newPoiSheet(org.apache.poi.ss.usermodel.Sheet delegate);

}
