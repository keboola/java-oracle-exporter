package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.UserException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONObject;
import org.junit.AfterClass;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

public class BaseTest {

    private static String dbPort;
    private static String dbHost;
    private static String dbUser;
    private static String dbPassword;
    private static String dbDatabase;
    private static Connection connection;
    protected static String query;
    protected static String outputFile;

    protected String createTemporaryConfigFile(String configFile) throws IOException, ApplicationException {
        String jsonString;
        try {
            byte[] encoded;
            encoded = Files.readAllBytes(Paths.get(configFile));
            jsonString = new String(encoded, "utf-8");
        } catch (IOException ex) {
            throw new ApplicationException("Configuration file is invalid", ex);
        }
        JSONObject baseObj = new JSONObject(jsonString);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        outputFile = paramsObj.getString("outputFile");
        query = paramsObj.getString("query");
        JSONObject dbobj = new JSONObject();

        dbobj.put("host", System.getenv("DB_HOST"));
        dbobj.put("port", System.getenv("DB_PORT"));
        dbobj.put("user", System.getenv("DB_USER"));
        dbobj.put("#password", System.getenv("DB_PASSWORD"));
        dbobj.put("database", System.getenv("DB_DATABASE"));
        paramsObj.put("db", dbobj);
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        try (FileWriter file = new FileWriter("tmp.json")) {
            file.write(baseObj.toString());
        }
        return "tmp.json";
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
            connection = DriverManager.getConnection(connectionString.toString(), dbUser, dbPassword);
        } catch (SQLException ex) {
            throw new UserException("Connection error: " + ex.getMessage(), ex);
        }
    }

    protected void setupDataTable(String testFile, String tableName) throws Exception {

        dbPort = System.getenv("DB_PORT");
        dbHost = System.getenv("DB_HOST");
        dbUser = System.getenv("DB_USER");
        dbPassword = System.getenv("DB_PASSWORD");
        dbDatabase = System.getenv("DB_DATABASE");

        connectDb();

        ClassLoader classLoader = getClass().getClassLoader();
        Reader reader = Files.newBufferedReader(Paths.get(classLoader.getResource(testFile).toURI()));

        CSVParser csvParser = new CSVParser(reader, CSVFormat.RFC4180.withFirstRecordAsHeader().withTrim());
        try {
            connectDb();
            dropTableIfExists(tableName);
            int cnt = 0;
            Map<String, Integer> header = csvParser.getHeaderMap();
            String headerSql = "create table " + tableName + " (";
            for (Map.Entry<String, Integer> entry : header.entrySet()) {
                if (cnt > 0) {
                    headerSql += ", ";
                }
                headerSql += "\"" + entry.getKey() + "\" varchar(64)";
                cnt++;
            }
            headerSql += ")";
            Statement headerStatement = connection.createStatement();
            headerStatement.execute(headerSql);

            for (CSVRecord csvRecord : csvParser) {
                Iterator iter = csvRecord.iterator();
                String sql = "insert into " + tableName + " values (";
                while (iter.hasNext()) {
                    sql += "'" + iter.next() + "'";
                    if (iter.hasNext()) {
                        sql += ", ";
                    }
                }
                sql += ")";
                Statement statement = connection.createStatement();
                statement.execute(sql);
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
                "   EXECUTE IMMEDIATE 'DROP TABLE " + tableName + "';\n" +
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
