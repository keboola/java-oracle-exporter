package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class ClobTest extends BaseTest {

    @Before
    public void setupTables() throws Exception {
        super.setupClobTable("clob/clobtest.csv", "clobtest");
    }

    @Test
    public void testEscapingFile() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("clob/clobtest.csv").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("clob/config.json").toURI();
        createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString(), "tmp.json");
        String[] args = {"export", "tmp.json"};
        app.main(args);

        File output = new File(outputFile);

        assertTrue(
            "Output file contents do not match expected",
            FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8")
        );
    }
}
