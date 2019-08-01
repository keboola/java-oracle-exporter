package com.keboola.tableexporter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;

import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class ColumnDataTest extends BaseTest {

    private ResultSet resultSetMock;
    private LinkedHashMap expectedOutput;

    public ColumnDataTest(ResultSet resultSetMock, LinkedHashMap expectedOutput) {
        this.resultSetMock = resultSetMock;
        this.expectedOutput = expectedOutput;
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> data() throws SQLException {

        return Arrays.asList(new Object[][] {
                { getResultSetMock("normalTest"), getExpectedOutput("normalTest") },
                { getResultSetMock("nullableTest"), getExpectedOutput("nullableTest") }
        });
    }

    @Test
    public void testColumnData() throws SQLException {
        MetaFetcher mf = new MetaFetcher(null);
        LinkedHashMap output = mf.getColumnData(resultSetMock);
        Assert.assertEquals(expectedOutput, output);
    }

    private static ResultSet getResultSetMock(String testName) throws SQLException {
        ResultSet resultSetMock = mock(ResultSet.class);
        switch (testName) {
            case "normalTest":
                when(resultSetMock.getString("COLUMN_NAME")).thenReturn("column_name");
                when(resultSetMock.getString("DATA_TYPE")).thenReturn("VARCHAR2");
                when(resultSetMock.getInt("COLUMN_ID")).thenReturn(1);
                when(resultSetMock.getString("CHAR_LENGTH")).thenReturn("32");
                when(resultSetMock.getString("NULLABLE")).thenReturn("N");
                break;
            case "nullableTest":
                when(resultSetMock.getString("COLUMN_NAME")).thenReturn("column_name");
                when(resultSetMock.getString("DATA_TYPE")).thenReturn("VARCHAR2");
                when(resultSetMock.getInt("COLUMN_ID")).thenReturn(1);
                when(resultSetMock.getString("CHAR_LENGTH")).thenReturn("32");
                when(resultSetMock.getString("NULLABLE")).thenReturn("Y");
                break;
        }
        return resultSetMock;
    }

    private static LinkedHashMap getExpectedOutput(String testName) {
        LinkedHashMap output = new LinkedHashMap();
        switch (testName) {
            case "normalTest":
                output.put("name", "column_name");
                output.put("sanitizedName", "column_name");
                output.put("type", "VARCHAR2");
                output.put("nullable", false);
                output.put("length", "32");
                output.put("ordinalPosition", 1);
                output.put("primaryKey", false);
                output.put("uniqueKey", false);
                break;
            case "nullableTest":
                output.put("name", "column_name");
                output.put("sanitizedName", "column_name");
                output.put("type", "VARCHAR2");
                output.put("nullable",  true);
                output.put("length", "32");
                output.put("ordinalPosition", 1);
                output.put("primaryKey", false);
                output.put("uniqueKey", false);
                break;
        }
        return output;
    }
}
