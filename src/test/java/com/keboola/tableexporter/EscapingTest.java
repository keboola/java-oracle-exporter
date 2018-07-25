package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class EscapingTest extends BaseTest {

    @Before
    public void setupTables() throws Exception {
        String[] colNames = {"col1", "col2"};
        super.setupDataTable("escaping/escaping.csv", "escaping", colNames);
    }

    @Test
    public void testEscapingFile() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("escaping/escaping.csv").toURI());
        File expectedManifest = new File(classLoader.getResource("escaping/escaping.csv.manifest").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("escaping/config.json").toURI();
        String tmpConfig = createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());
        String[] args = {tmpConfig};
        app.main(args);

        File output = new File(outputFile);
        File manifest = new File(outputFile + ".manifest");
        assertTrue("The data files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
        assertTrue("The manifests differ!", FileUtils.contentEqualsIgnoreEOL(expectedManifest, manifest, "UTF-8"));
    }

    @Test
    public void testEmptyResultSet() throws IOException, URISyntaxException, ApplicationException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedManifest = new File(classLoader.getResource("escaping/emptyResult.csv.manifest").toURI());

        Application app = new Application();

        URI configUri = classLoader.getResource("escaping/emptyResultConfig.json").toURI();
        String tmpConfig = createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());
        String[] args = {tmpConfig};
        app.main(args);

        File manifest = new File(outputFile + ".manifest");
        File outputData = new File(outputFile);
        assertFalse(outputData.exists());

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedManifest, manifest, "UTF-8"));
    }
}
