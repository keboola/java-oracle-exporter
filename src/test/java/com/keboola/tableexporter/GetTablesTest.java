package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.UserException;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.json.JSONArray;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
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

        try {
            JSONArray outputJson = new JSONArray(new String(Files.readAllBytes(Paths.get("output/tables.json"))));
            JSONArray expectedJson = new JSONArray(new String(Files.readAllBytes(Paths.get(expectedFile.toString()))));
            assertEquals(outputJson, expectedJson);
        } catch (IOException ioException) {
            System.err.println("IO Exception: " + ioException.getMessage());
        }
    }
}
