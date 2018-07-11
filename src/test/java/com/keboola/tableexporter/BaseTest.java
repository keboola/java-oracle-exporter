package com.keboola.tableexporter;

import com.keboola.tableexporter.ApplicationException;
import com.keboola.tableexporter.UserException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class BaseTest {

    private static String dbPort;
    private static String dbHost;
    private static String dbUser;
    private static String dbPassword;
    private static String dbDatabase;
    private static Connection connection;
    protected static String query;
    protected static String outputFile;

    protected void readConfigFile(String configFile) throws ApplicationException {
        System.out.println("Processing configuration file");
        String jsonString;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            byte[] encoded;
            encoded = Files.readAllBytes(Paths.get(classLoader.getResource(configFile).toURI()));
            jsonString = new String(encoded, "utf-8");
        } catch (IOException ex) {
            throw new ApplicationException("Configuration file is invalid", ex);
        } catch (URISyntaxException e) {
            throw new ApplicationException("Could not find config file", e);
        }
        JSONObject obj = new JSONObject(jsonString);
        query = obj.getJSONObject("parameters").getString("query");
        outputFile = obj.getJSONObject("parameters").getString("outputFile");
    }

    private static void connectDb() throws ApplicationException, UserException {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException ex) {
            throw new ApplicationException("Driver error", ex);
        }
        try {
            StringBuilder connectionString = new StringBuilder();
            connectionString
                    .append("jdbc:oracle:thin:@")
                    .append(dbHost)
                    .append(":")
                    .append(dbPort)
                    .append(":")
                    .append(dbDatabase);
            System.out.println("Connecting to: " + connectionString);
            connection = DriverManager.getConnection(connectionString.toString(), dbUser, dbPassword);
        } catch (SQLException ex) {
            throw new UserException("Connection error: " + ex.getMessage(), ex);
        }
    }

    protected void setupDataTable(String testFile, String tableName) throws Exception {

        System.out.println("System Env Vars");
        for (Map.Entry<String, String> entry : System.getenv().entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }

        dbPort = System.getenv("DB_PORT");
        dbHost = System.getenv("DB_HOST");
        dbUser = System.getenv("DB_USER");
        dbPassword = System.getenv("DB_PASSWORD");
        dbDatabase = System.getenv("DB_DATABASE");

        System.out.println("Host: " + dbHost);
        connectDb();

        ClassLoader classLoader = getClass().getClassLoader();
        Reader reader = Files.newBufferedReader(Paths.get(classLoader.getResource(testFile).toURI()));

        CSVParser csvParser = new CSVParser(reader, CSVFormat.RFC4180.withFirstRecordAsHeader().withTrim());
        try {
            connectDb();
            dropTableIfExists(tableName);
            int cnt = 0;
            for (CSVRecord csvRecord : csvParser) {
                if (cnt == 0) {
                    Iterator iter = csvRecord.iterator();
                    String sql = "create table " + tableName + " (";
                    while (iter.hasNext()) {
                        sql += iter.next() + " varchar(64)";
                        if (iter.hasNext()) {
                            sql += ", ";
                        }
                    }
                    sql += ")";
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                } else {
                    Iterator iter = csvRecord.iterator();
                    String sql = "insert into " + tableName + " values (";
                    while (iter.hasNext()) {
                        sql += "'" + iter.next() + "'";
                        if (iter.hasNext()) {
                            sql += ", ";
                        }
                    }
                    sql += ")";
                    System.out.println(sql);
                    Statement statement = connection.createStatement();
                    statement.execute(sql);
                }
                cnt++;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        connection.close();
    }


    private void dropTableIfExists(String tableName) throws SQLException {
        String sql = "BEGIN\n" +
                "   EXECUTE IMMEDIATE 'DROP TABLE %s';\n" +
                "EXCEPTION\n" +
                "   WHEN OTHERS THEN\n" +
                "      IF SQLCODE != -942 THEN\n" +
                "         RAISE;\n" +
                "      END IF;\n" +
                "END;";
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
    }
}
