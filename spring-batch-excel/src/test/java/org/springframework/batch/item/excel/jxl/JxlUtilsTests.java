/*
 * Copyright 2006-2014 the original author or authors.
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
package org.springframework.batch.item.excel.jxl;

import jxl.Cell;
import jxl.Workbook;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link org.springframework.batch.item.excel.jxl.JxlUtils}.
 *
 * @author Marten Deinum
 *
 */
public class JxlUtilsTests {

    private final Cell cell1 = Mockito.mock(Cell.class);
    private final Cell cell2 = Mockito.mock(Cell.class);
    private final Cell cell3 = Mockito.mock(Cell.class);
    private final Cell cell4 = Mockito.mock(Cell.class);

    private final Workbook workbook = Mockito.mock(Workbook.class);

    @Before
    public void setup() {
        Mockito.when(this.cell1.getContents()).thenReturn("foo");
        Mockito.when(this.cell2.getContents()).thenReturn(" ");
        Mockito.when(this.cell3.getContents()).thenReturn("");
        Mockito.when(this.cell4.getContents()).thenReturn(null);
    }

    /**
     * Test the {@link org.springframework.batch.item.excel.jxl.JxlUtils#isEmpty( jxl.Cell)} method.
     */
    @Test
    public void checkIfCellsAreEmpty() {
        Assert.assertFalse("Cell[1] should not be empty", JxlUtils.isEmpty(this.cell1));
        Assert.assertTrue("Cell[2] should be empty", JxlUtils.isEmpty(this.cell2));
        Assert.assertTrue("Cell[3] should be empty", JxlUtils.isEmpty(this.cell3));
        Assert.assertTrue("Cell[4] should be empty", JxlUtils.isEmpty(this.cell4));
        Assert.assertTrue("[null] should be empty", JxlUtils.isEmpty((Cell) null));
    }

    /**
     * Test the {@link JxlUtils#isEmpty( jxl.Cell[])} method.
     */
    @Test
    public void checkIfRowIsEmpty() {
        Assert.assertTrue("[null] should be empty", JxlUtils.isEmpty((Cell[]) null));
        Assert.assertTrue("[null] should be empty", JxlUtils.isEmpty(new Cell[0]));
        Assert.assertFalse("Cell[1] should not be empty",
                JxlUtils.isEmpty(new Cell[]{this.cell1, this.cell2, this.cell3}));
        Assert.assertTrue("Cell[2] should be empty", JxlUtils.isEmpty(new Cell[]{this.cell2, this.cell3, null}));
    }

    /**
     * Test the {@link JxlUtils#hasSheets( jxl.Workbook)} method.
     */
    @Test
    public void checkIfWorkbookHasSheets() {
        Assert.assertFalse("[null] doesn't have sheets.", JxlUtils.hasSheets(null));
        Mockito.when(this.workbook.getNumberOfSheets()).thenReturn(5);
        Assert.assertTrue("Workbook should have sheets.", JxlUtils.hasSheets(this.workbook));
        Mockito.when(this.workbook.getNumberOfSheets()).thenReturn(0);
        Assert.assertFalse("Workbook shouldn't have sheets.", JxlUtils.hasSheets(this.workbook));

    }

    @Test
    public void extractingContent() {
        Assert.assertTrue("[null] should give empty array", JxlUtils.extractContents(null).length == 0);
    }

}
