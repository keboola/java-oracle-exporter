package com.keboola.tableexporter;

import com.keboola.tableexporter.Application;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class EscapingTest extends BaseTest {

    @Before
    public void setupTables() throws Exception {
        super.setupDataTable("escaping/escaping.csv", "escaping");
    }

    @Test
    public void testEscapingFile() throws IOException
    {
        File expectedFile = new File("../resources/escaping/escaping.csv");
        File noFile = new File("../../../data/test.csv");
        Application app = new Application();
        String[] args = {"escaping/escaping.csv"};
        app.main(args);
        assertTrue("The files differ!", FileUtils.contentEquals(expectedFile, noFile));
    }
}
