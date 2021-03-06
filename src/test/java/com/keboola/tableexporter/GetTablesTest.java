package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class GetTablesTest extends BaseTest {

    @Test
    public void testGetTables() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedResults.json").toURI());

        URI configUri = classLoader.getResource("getTables/config.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};
        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    @Test
    public void testGetSingleTable() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedSingleTableResults.json").toURI());

        URI configUri = classLoader.getResource("getTables/singleTableConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};
        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    @Test
    public void testGetSingleView() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedSingleViewResults.json").toURI());

        URI configUri = classLoader.getResource("getTables/singleViewConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};
        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }


    @Test
    public void testGetOnlyTables() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedOnlyTablesResults.json").toURI());

        URI configUri = classLoader.getResource("getTables/onlyTablesConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};
        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    @Test
    public void testGetSingleTableNoColumns() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedSingleTableNoColumnsResults.json").toURI());

        URI configUri = classLoader.getResource("getTables/singleTableNoColumnsConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};
        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }
}
