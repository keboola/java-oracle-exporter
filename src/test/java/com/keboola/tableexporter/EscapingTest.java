package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;


public class EscapingTest extends BaseTest {

    @Before
    public void setupTables() throws Exception {
        super.setupDataTable("escaping/escaping.csv", "escaping");
    }

    @Test
    public void testEscapingFile() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("escaping/escaping.csv").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("escaping/config.json").toURI();
        createTemporaryConfigFile("tmp.json", Paths.get(configUri).toAbsolutePath().toString());
        String[] args = {"tmp.json"};
        app.main(args);

        File output = new File(Application.OUTPUT_DIR + Application.DATA_OUTPUT_FILE);

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    @Test
    public void testEmptyResultSet() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("escaping/emptyResult.csv").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("escaping/emptyResultConfig.json").toURI();
        createTemporaryConfigFile("tmp.json", Paths.get(configUri).toAbsolutePath().toString());
        String[] args = {"tmp.json"};
        app.main(args);

        File output = new File(Application.OUTPUT_DIR + Application.DATA_OUTPUT_FILE);

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    @Test
    public void testHeaderlessOutput() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("escaping/headerless-escaping.csv").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("escaping/config.json").toURI();
        createTemporaryConfigFile("tmp.json", Paths.get(configUri).toAbsolutePath().toString());
        String[] args = {"tmp.json", "false"};
        app.main(args);

        File output = new File(Application.OUTPUT_DIR + Application.DATA_OUTPUT_FILE);

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }
}
