package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.UserException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONObject;
import org.json.JSONArray;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;

import java.io.*;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
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
    protected static String outputFile;
    private static Connection connection;
    protected SecurityManager securityManager;

    protected static class ExitException extends SecurityException
    {
        public final int status;
        public ExitException(int status)
        {
            super("Exit called.");
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager
    {
        @Override
        public void checkPermission(Permission perm)
        {
            // allow anything.
        }
        @Override
        public void checkPermission(Permission perm, Object context)
        {
            // allow anything.
        }
        @Override
        public void checkExit(int status)
        {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }

    @Before
    public void setUp() throws Exception
    {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @After
    public void tearDown() throws Exception
    {
        System.setSecurityManager(securityManager); // restore
    }

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

    @Rule
    public final SystemErrRule systemErrRule = new SystemErrRule().enableLog();

    protected String createTemporaryConfigFile(String inputConfigFile) throws IOException, ApplicationException {
        JSONObject baseObj = getJsonConfigFromFile(inputConfigFile);
        JSONObject paramsObj = baseObj.getJSONObject("parameters");
        if (paramsObj.has("outputFile")) {
            outputFile = paramsObj.getString("outputFile");
        }
        JSONObject dbObj = new JSONObject();
        if (paramsObj.has("db")) {
            dbObj = paramsObj.getJSONObject("db");
        }
        paramsObj.put("db", getDbJsonNode(dbObj));
        baseObj.remove("parameters");
        baseObj.put("parameters", paramsObj);
        File outputConfigFile = File.createTempFile("config", ".json");
        writeJsonConfigToFile(baseObj, outputConfigFile.getAbsolutePath());
        return outputConfigFile.getAbsolutePath();
    }

    protected String createTemporaryTnsnameFile() throws IOException {
        Path dir = Files.createTempDirectory("tnsnames");
        Path fileToCreatePath = dir.resolve("tnsnames.ora");
        Path outputFile = Files.createFile(fileToCreatePath);

        String content = "XE = (DESCRIPTION = (ADDRESS = (PROTOCOL = tcp)(HOST=" + System.getenv("DB_HOST") + ")(PORT = " + System.getenv("DB_PORT") + "))(CONNECT_DATA = (SID = " + System.getenv("DB_DATABASE") + ")(SERVICE_NAME = XE)))";
        writeTnsnamesToFile(content, outputFile.toString());
        return dir.toString();
    }

    protected String createTemporaryInvalidTnsnameFile() throws IOException {
        Path dir = Files.createTempDirectory("invalidtnsnames");
        Path fileToCreatePath = dir.resolve("tnsnames.ora");
        Path outputFile = Files.createFile(fileToCreatePath);

        String content = "XE = (DESCRIPTION = (ADDRESS = (PROTOCOL = invalidProtocol)))";
        writeTnsnamesToFile(content, outputFile.toString());
        return dir.toString();
    }

    protected JSONObject getJsonConfigFromFile(String fileName) throws IOException {
        String jsonString;
        byte[] encoded;
        encoded = Files.readAllBytes(Paths.get(fileName));
        jsonString = new String(encoded, "utf-8");
        return new JSONObject(jsonString);
    }

    protected void writeJsonConfigToFile(JSONObject jsonConfig, String fileName) throws IOException {
        try (FileWriter file = new FileWriter(fileName)) {
            file.write(jsonConfig.toString());
        }
    }

    protected void writeTnsnamesToFile(String content, String fileName) throws IOException {
        try (FileWriter file = new FileWriter(fileName)) {
            file.write(content);
        }
    }

    protected JSONObject getDbJsonNode(JSONObject dbNode) {
        dbNode.put("host", System.getenv("DB_HOST"));
        dbNode.put("port", System.getenv("DB_PORT"));
        dbNode.put("user", System.getenv("DB_USER"));
        dbNode.put("#password", System.getenv("DB_PASSWORD"));
        dbNode.put("database", System.getenv("DB_DATABASE"));
        dbNode.put("tnsnamesService", "XE");

        return dbNode;
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

    protected void setupClobTable(String testFile, String tableName) throws Exception {

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
            String headerSql = "create table clobtest (\"index\" integer, xml CLOB)";
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

    protected void setupSynonyms() throws Exception {
        dbPort = System.getenv("DB_PORT");
        dbHost = System.getenv("DB_HOST");
        dbUser = System.getenv("DB_USER");
        dbPassword = System.getenv("DB_PASSWORD");
        dbDatabase = System.getenv("DB_DATABASE");

        connectDb();

        Statement statement = connection.createStatement();
        createUserIfNotExists("SETUPUSER", "setuppassword");
        statement.execute("GRANT CONNECT TO SETUPUSER");
        statement.execute("GRANT CREATE SESSION to SETUPUSER");
        statement.execute("GRANT ALL PRIVILEGES TO SETUPUSER");
        statement.execute("GRANT UNLIMITED TABLESPACE TO SETUPUSER");

        /*
        createUserIfNotExists("FETCHER", "fetchpassword");
        statement.execute("GRANT CONNECT TO FETCHER");
        statement.execute("GRANT CREATE SESSION to FETCHER");
        statement.execute("GRANT ALL PRIVILEGES TO FETCHER");
        statement.execute("GRANT UNLIMITED TABLESPACE TO FETCHER");
        */
        closeConnection();

        dbUser = "SETUPUSER";
        dbPassword = "setuppassword";

        connectDb();

        dropTableIfExists("SETUPUSER.synonymtest");
        statement = connection.createStatement();
        statement.execute("create table SETUPUSER.synonymtest (\"index\" integer, name varchar2(64))");
        statement.execute("CREATE OR REPLACE SYNONYM synonymlisted FOR SETUPUSER.synonymtest");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        try {
            connection.close();
        } catch (NullPointerException e) { }
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

    private void createUserIfNotExists(String username, String password) throws SQLException {

        String sql = "declare\n" +
                "userexist integer;\n" +
                "begin\n" +
                "  select count(*) into userexist from all_users where username='" + username + "';\n" +
                "  if (userexist = 0) then\n" +
                "    execute immediate 'create user " + username + " identified by " + password + "';\n" +
                "  end if;\n" +
                "end;";
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
    }
}
