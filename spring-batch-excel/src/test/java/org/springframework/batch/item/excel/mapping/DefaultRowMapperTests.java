/*
 * Copyright 2011-2014 the original author or authors.
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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.item.excel.Sheet;
import org.springframework.batch.item.excel.transform.RowTokenizer;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import static org.mockito.Matchers.any;

/**
 * Tests for {@link DefaultRowMapper}.
 * @author Marten Deinum
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRowMapperTests {

    @Mock
    private FieldSetMapper fieldSetMapper;

    @Mock
    private RowTokenizer rowTokenizer;

    @Test(expected = IllegalArgumentException.class)
    public void nullRowTokenizerShouldLeadToException() throws Exception {
        final DefaultRowMapper mapper = new DefaultRowMapper();
        mapper.setRowTokenizer(null);
        mapper.setFieldSetMapper(this.fieldSetMapper);
        mapper.afterPropertiesSet();
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullFieldSetMapperShouldLeadToException() throws Exception {
        final DefaultRowMapper mapper = new DefaultRowMapper();
        mapper.setRowTokenizer(this.rowTokenizer);
        mapper.setFieldSetMapper(null);
        mapper.afterPropertiesSet();
    }

    @Test
    public void foo() throws Exception {
        final DefaultRowMapper mapper = new DefaultRowMapper();
        mapper.setRowTokenizer(this.rowTokenizer);
        mapper.setFieldSetMapper(this.fieldSetMapper);
        final FieldSet fs = Mockito.mock(FieldSet.class);
        final Object result = new Object();
        Mockito.when(this.rowTokenizer.tokenize(any(Sheet.class), any(String[].class))).thenReturn(fs);
        Mockito.when(this.fieldSetMapper.mapFieldSet(fs)).thenReturn(result);
        Assert.assertEquals(result, mapper.mapRow(null, null, 0));
        Mockito.verify(this.rowTokenizer, Mockito.times(1)).tokenize(any(Sheet.class), any(String[].class));
        Mockito.verify(this.fieldSetMapper, Mockito.times(1)).mapFieldSet(fs);
    }

}
