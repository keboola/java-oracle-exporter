package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestConnectionTest extends BaseTest {
    @Test
    public void testTestConnectionTnsnames() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"testConnection", tmpFile, createTemporaryTnsnameFile()};

        Application.main(args);

        String expectedLog = "Connecting user \"system\". Using service name \"XE\" from tnsnames.ora.\n" +
                "Executing query \"select sysdate from dual\"\n";
        assertEquals(expectedLog, systemOutRule.getLog());
    }

    @Test
    public void testTestConnectionInvalidTnsnames() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"testConnection", tmpFile, createTemporaryInvalidTnsnameFile()};

        try {
            Application.main(args);
            fail("Exception expected");
        } catch (ExitException exception) {
            assertEquals(1, exception.status);

            String expectedLog = "TNSNAMES is probably invalid, please check it, especially SID, ServiceName and parentheses. Connection error: IO Error: The Network Adapter could not establish the connection";
            assertTrue(systemErrRule.getLog().contains(expectedLog));
        }
    }

    @Test
    public void testTestConnectionCredentials() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString());

        String[] args = {"testConnection", tmpFile};

        Application.main(args);

        String expectedLog = "Connecting user \"system\" to database \"XE\" at \"oracle\" on port \"1521\"\n" +
                "Executing query \"select sysdate from dual\"\n";
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

        try {
            Application.main(args);
        } catch (ExitException exception) {
            assertEquals(1, exception.status);
        }
    }

    @Test
    public void testMissingCredential() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        Application app = new Application();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        String tmpFile = createTemporaryConfigFileWithMissingCredential(
                Paths.get(configUri).toAbsolutePath().toString()
        );

        String[] args = {"testConnection", tmpFile};

        try {
            Application.main(args);
        } catch (ExitException exception) {
            assertEquals(1, exception.status);

            String expectedErrorLog = "JSONObject[\"database\"] not found.\n";
            assertEquals(expectedErrorLog, systemErrRule.getLog());
        }
    }

    @Test
    public void testProxyUserInvalid() throws IOException {
        JSONObject baseObj = new JSONObject();
        JSONObject paramsObj = new JSONObject();
        JSONObject dbObj = new JSONObject();
        if (paramsObj.has("db")) {
            dbObj = paramsObj.getJSONObject("db");
        }
        JSONObject dbNode = getDbJsonNode(dbObj);
        dbNode.put("proxyUser", "invalid-proxy-user");
        paramsObj.put("db", dbNode);
        baseObj.put("parameters", paramsObj);

        File outputConfigFile = File.createTempFile("config", ".json");
        String tmpFile = outputConfigFile.getAbsolutePath();
        writeJsonConfigToFile(baseObj, tmpFile);

        String[] args = {"testConnection", tmpFile};

        try {
            Application.main(args);
            fail("Exception expected");
        } catch (ExitException exception) {
            assertEquals(1, exception.status);

            String expectedLog = "Connecting user \"system\" to database \"XE\" at \"oracle\" on port \"1521\"\n" +
                    "Proxy user = \"invalid-proxy-user\"\n";
            assertEquals(expectedLog, systemOutRule.getLog());

            // The driver rejects the invalid proxy user; the exact message is driver-specific.
            assertTrue(systemErrRule.getLog().startsWith("Proxy user error: "));
        }
    }

    protected String createTemporaryConfigFileWithInvalidCredentials(String inputConfigFile) throws IOException, ApplicationException {
        JSONObject baseObj = getJsonConfigFromFile(inputConfigFile);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        JSONObject dbObj = new JSONObject();
        if (paramsObj.has("db")) {
            dbObj = paramsObj.getJSONObject("db");
        }
        JSONObject dbNode = getDbJsonNode(dbObj);
        dbNode.put("#password", "invalid_password");
        paramsObj.put("db", dbNode);
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        File outputConfigFile = File.createTempFile("config", ".json");
        writeJsonConfigToFile(baseObj, outputConfigFile.getAbsolutePath());
        return outputConfigFile.getAbsolutePath();
    }

    protected String createTemporaryConfigFileWithMissingCredential(String inputConfigFile) throws IOException, ApplicationException {
        JSONObject baseObj = getJsonConfigFromFile(inputConfigFile);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        JSONObject dbObj = new JSONObject();
        if (paramsObj.has("db")) {
            dbObj = paramsObj.getJSONObject("db");
        }
        JSONObject dbNode = getDbJsonNode(dbObj);
        dbNode.remove("database");
        paramsObj.put("db", dbNode);
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        File outputConfigFile = File.createTempFile("config", ".json");
        writeJsonConfigToFile(baseObj, outputConfigFile.getAbsolutePath());
        return outputConfigFile.getAbsolutePath();
    }
}
