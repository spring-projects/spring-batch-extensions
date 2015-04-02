package org.springframework.batch.item.excel.support.rowset;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.item.excel.Sheet;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link DefaultRowSetMetaData}
 *
 * @author Marten Deinum
 * @since 0.5.0
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRowSetMetaDataTest {

    private static final String[] COLUMNS = {"col1", "col2", "col3"};

    private DefaultRowSetMetaData rowSetMetaData;

    @Mock
    private Sheet sheet;

    @Mock
    private ColumnNameExtractor columnNameExtractor;

    @Before
    public void setup() {
        rowSetMetaData = new DefaultRowSetMetaData(sheet, columnNameExtractor);
    }

    @Test
    public void shouldMatchColumnCountWithNumberOfHeaders() {

        when(columnNameExtractor.getColumnNames(sheet)).thenReturn(COLUMNS);
        int numColumns = rowSetMetaData.getColumnCount();

        assertThat(numColumns, is(COLUMNS.length));
    }

    @Test
    public void shouldReturnColumnsFromColumnNameExtractor() {

        when(columnNameExtractor.getColumnNames(sheet)).thenReturn(COLUMNS);

        String[] names = rowSetMetaData.getColumnNames();

        assertThat(names.length, is(3));
        assertThat(names, arrayContaining("col1", "col2", "col3"));
        verify(columnNameExtractor, times(1)).getColumnNames(sheet);
        verifyNoMoreInteractions(sheet, columnNameExtractor);
    }

    @Test
    public void shouldGetAndReturnNameOfTheSheet() {

        when(sheet.getName()).thenReturn("testing123");

        String name = rowSetMetaData.getSheetName();

        assertThat(name, is("testing123"));
        verify(sheet, times(1)).getName();
        verifyNoMoreInteractions(sheet);
    }

    @Test
    public void shouldGetCorrectColumnName() {

        when(columnNameExtractor.getColumnNames(sheet)).thenReturn(COLUMNS);

        assertThat(rowSetMetaData.getColumnName(0), is("col1"));
        assertThat(rowSetMetaData.getColumnName(1), is("col2"));
        assertThat(rowSetMetaData.getColumnName(2), is("col3"));

    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowArrayIndexOutOfBoundsExceptionWhenIdxIsTooLarge() {

        when(columnNameExtractor.getColumnNames(sheet)).thenReturn(COLUMNS);

        rowSetMetaData.getColumnName(900);
    }

}