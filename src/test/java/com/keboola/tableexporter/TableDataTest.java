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
public class TableDataTest extends BaseTest {

    private ResultSet resultSetMock;
    private LinkedHashMap expectedOutput;

    public TableDataTest(ResultSet resultSetMock, LinkedHashMap expectedOutput) {
        this.resultSetMock = resultSetMock;
        this.expectedOutput = expectedOutput;
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> data() throws SQLException {

        return Arrays.asList(new Object[][] {
                { getResultSetMock("normalTest"), getExpectedOutput("normalTest") },
                { getResultSetMock("noRowCountTest"), getExpectedOutput("noRowCountTest") }
        });
    }

    @Test
    public void testTableData() throws SQLException {
        MetaFetcher mf = new MetaFetcher(null);
        LinkedHashMap output = mf.getTableData(resultSetMock);
        Assert.assertEquals(expectedOutput, output);
    }

    private static ResultSet getResultSetMock(String testName) throws SQLException {
        ResultSet resultSetMock = mock(ResultSet.class);
        switch (testName) {
            case "normalTest":
                when(resultSetMock.getString("TABLE_NAME")).thenReturn("someTableName");
                when(resultSetMock.getString("TABLESPACE_NAME")).thenReturn("sometablespace");
                when(resultSetMock.getString("OWNER")).thenReturn("something");
                when(resultSetMock.getString("NUM_ROWS")).thenReturn("127");
                when(resultSetMock.getLong("NUM_ROWS")).thenReturn(127L);
                break;
            case "noRowCountTest":
                when(resultSetMock.getString("TABLE_NAME")).thenReturn("someTableName");
                when(resultSetMock.getString("TABLESPACE_NAME")).thenReturn("sometablespace");
                when(resultSetMock.getString("OWNER")).thenReturn("something");
                break;
        }
        return resultSetMock;
    }

    private static LinkedHashMap getExpectedOutput(String testName) {
        LinkedHashMap output = new LinkedHashMap();
        switch (testName) {
            case "normalTest":
                output.put("name", "someTableName");
                output.put("tablespaceName", "sometablespace");
                output.put("schema", "something");
                output.put("owner", "something");
                output.put("rowCount", 127L);
                break;
            case "noRowCountTest":
                output.put("name", "someTableName");
                output.put("tablespaceName", "sometablespace");
                output.put("schema", "something");
                output.put("owner", "something");
                break;
        }
        return output;
    }

}
