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

        Application app = new Application();

        URI configUri = classLoader.getResource("getTables/emptyConfig.json").toURI();

        super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString(), "tmp.json");

        String[] args = {"getTables", "tmp.json"};
        app.main(args);

        File output = new File("output/tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }
}
