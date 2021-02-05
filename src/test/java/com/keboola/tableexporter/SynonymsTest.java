package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class SynonymsTest extends BaseTest {

    @Test
    public void testGetTablesWithSynonyms() throws Exception, IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();
        File expectedFile = new File(classLoader.getResource("getTables/expectedResultsWithSynonyms.json").toURI());

        URI configUri = classLoader.getResource("getTables/config.json").toURI();

        String tmpFile = createTemporaryConfigFileWithDifferentCredentials(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"getTables", tmpFile};

        setupSynonyms();

        Application.main(args);

        File output = new File("tables.json");

        assertTrue("The files differ!", FileUtils.contentEqualsIgnoreEOL(expectedFile, output, "UTF-8"));
    }

    protected String createTemporaryConfigFileWithDifferentCredentials(String inputConfigFile) throws IOException, ApplicationException {
        JSONObject baseObj = getJsonConfigFromFile(inputConfigFile);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        JSONObject dbObj = getDbJsonNode();
        dbObj.put("user", "SETUPUSER");
        dbObj.put("#password", "setuppassword");
        paramsObj.put("db", dbObj);
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        File outputConfigFile = File.createTempFile("config", ".json");
        writeJsonConfigToFile(baseObj, outputConfigFile.getAbsolutePath());
        return outputConfigFile.getAbsolutePath();
    }
}
