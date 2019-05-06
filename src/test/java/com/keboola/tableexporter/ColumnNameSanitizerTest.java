package com.keboola.tableexporter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class ColumnNameSanitizerTest {

    private String columnName;
    private String expectedSanitization;

    public ColumnNameSanitizerTest(String columnName, String expectedSanitization) {
        this.columnName = columnName;
        this.expectedSanitization = expectedSanitization;
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> data() {

        return Arrays.asList(new Object[][] {
                { "normalName", "normalName" },
                { "_underscore",  "underscore"},
                { "s%mb-ol#s", "s_mb_ol_s" }
        });
    }

    @Test
    public void columnNameSanitizeTest() {
        String sanitizedName = MetaFetcher.columnNameSanitizer(columnName);
        Assert.assertEquals(sanitizedName, expectedSanitization);
    }
}
