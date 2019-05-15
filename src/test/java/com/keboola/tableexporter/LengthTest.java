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
public class LengthTest extends BaseTest {

    private ResultSet resultSetMock;
    private String expectedOutput;

    public LengthTest(ResultSet resultSetMock, String expectedOutput) {
        this.resultSetMock = resultSetMock;
        this.expectedOutput = expectedOutput;
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> data() throws SQLException {

        return Arrays.asList(new Object[][] {
                { getResultSetMock("varcharTest"), "64" },
                { getResultSetMock("decimalTest"), "12,2" },
                { getResultSetMock("noLengthTest"), null }
        });
    }

    @Test
    public void testLength() throws SQLException {
        MetaFetcher mf = new MetaFetcher(null);
        String output = mf.getColumnLength(resultSetMock);
        Assert.assertEquals(expectedOutput, output);
    }

    private static ResultSet getResultSetMock(String testName) throws SQLException {
        ResultSet resultSetMock = mock(ResultSet.class);
        switch (testName) {
            case "varcharTest":
                when(resultSetMock.getString("DATA_TYPE")).thenReturn("VARCHAR2");
                when(resultSetMock.getString("DATA_LENGTH")).thenReturn("64");
                break;
            case "decimalTest":
                when(resultSetMock.getString("DATA_TYPE")).thenReturn("DECIMAL");
                when(resultSetMock.getString("DATA_PRECISION")).thenReturn("12");
                when(resultSetMock.getString("DATA_SCALE")).thenReturn("2");
                break;
            case "noLengthTest":
                when(resultSetMock.getString("DATA_TYPE")).thenReturn("DATE");
                when(resultSetMock.getString("DATA_LENGTH")).thenReturn(null);
                break;
        }
        return resultSetMock;
    }
}
