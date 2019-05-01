package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.UserException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemOutRule;

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

        super.createTemporaryConfigFile(Paths.get(configUri).toAbsolutePath().toString(), "tmp.json");

        String[] args = {"testConnection", "tmp.json"};

        app.main(args);

        String expectedLog = "executing action testConnection\n" +
                "Processing configuration file tmp.json\n" +
                "Connecting user system to database xe at oracle\n" +
                "All done\n";
        assertEquals(expectedLog, systemOutRule.getLog());
    }

    @Test
    public void testInvalidConnection() throws IOException, URISyntaxException, ApplicationException {
        ClassLoader classLoader = getClass().getClassLoader();

        Application app = new Application();

        URI configUri = classLoader.getResource("testConnection/testConnectionConfig.json").toURI();

        super.createTemporaryConfigFileWithInvalidCredentials(
                Paths.get(configUri).toAbsolutePath().toString(),
                "tmp.json"
        );

        String[] args = {"testConnection", "tmp.json"};

        exit.expectSystemExitWithStatus(1);
        app.main(args);
    }
}
