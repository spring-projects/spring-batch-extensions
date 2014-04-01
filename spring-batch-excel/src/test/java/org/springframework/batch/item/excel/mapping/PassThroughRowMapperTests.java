/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel.mapping;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link PassThroughRowMapper}.
 * 
 * @author Marten Deinum
 *
 */
public class PassThroughRowMapperTests {

    private final PassThroughRowMapper rowMapper = new PassThroughRowMapper();

    @Test
    public void mapRowShouldReturnSameValues() throws Exception {
        final String[] row = new String[] { "foo", "bar", "baz" };

        assertArrayEquals(row, this.rowMapper.mapRow(null, row, 0));
    }

    @Test
    public void mapRowShouldReturnNull() throws Exception {
        assertNull(this.rowMapper.mapRow(null, null, 0));
    }

}
