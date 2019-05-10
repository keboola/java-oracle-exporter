package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.UserException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemOutRule;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TestConnectionTest extends BaseTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

    @Test
    public void testTestConnection() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        Application app = new Application();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"testConnection", tmpFile};

        app.main(args);

        String expectedLog = "executing action testConnection\n" +
                "Processing configuration file " + tmpFile + "\n" +
                "Connecting user system to database xe at oracle\n" +
                "All done\n";
        assertEquals(expectedLog, systemOutRule.getLog());
    }

    @Test
    public void testInvalidConnection() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        Application app = new Application();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = createTemporaryConfigFileWithInvalidCredentials(
                Paths.get(configUri).toAbsolutePath().toString()
        );

        String[] args = {"testConnection", tmpFile};

        exit.expectSystemExitWithStatus(1);
        app.main(args);
    }

    protected String createTemporaryConfigFileWithInvalidCredentials(String inputConfigFile) throws IOException, ApplicationException {
        JSONObject baseObj = getJsonConfigFromFile(inputConfigFile);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        JSONObject dbObj = getDbJsonNode();
        dbObj.put("#password", "invalid_password");
        paramsObj.put("db", dbObj);
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        File outputConfigFile = File.createTempFile("config", ".json");
        writeJsonConfigToFile(baseObj, outputConfigFile.getAbsolutePath());
        return outputConfigFile.getAbsolutePath();
    }
}
