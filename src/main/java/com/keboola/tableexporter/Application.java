package com.keboola.tableexporter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.CsvException;
import com.keboola.tableexporter.exception.UserException;
import org.json.JSONObject;

public class Application {

    public final static String OUTPUT_DIR = "output/";
    public final static String TABLES_OUTPUT_FILE = "tables.json";
    public final static String DATA_OUTPUT_FILE = "data.csv";

    private static String action;
    private static String dbPort;
    private static String dbHost;
    private static String dbUser;
    private static String dbPassword;
    private static String dbName;
    private static String query;
    private static Connection connection;
    private static boolean includeHeader;
    
    private static void readConfigFile(String configFile) throws ApplicationException {
        System.out.println("Processing configuration file " + configFile);
        String jsonString;
        try {
            byte[] encoded;
            Path configPath = Paths.get(configFile);
            encoded = Files.readAllBytes(configPath);
            jsonString = new String(encoded, "utf-8");
        } catch (IOException ex) {
            throw new ApplicationException("Configuration file is invalid", ex);
        }
        JSONObject obj = new JSONObject(jsonString);
        dbPort = obj.getJSONObject("parameters").getJSONObject("db").getString("port");
        dbHost = obj.getJSONObject("parameters").getJSONObject("db").getString("host");
        dbUser = obj.getJSONObject("parameters").getJSONObject("db").getString("user");
        dbPassword = obj.getJSONObject("parameters").getJSONObject("db").getString("#password");
        dbName = obj.getJSONObject("parameters").getJSONObject("db").getString("database");
        if (obj.getJSONObject("parameters").has("query")) {
            query = obj.getJSONObject("parameters").getString("query");
        }
    }    
    
    private static void connectDb() throws ApplicationException, UserException {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException ex) {
            throw new ApplicationException("Driver error", ex);
        }
        StringBuilder connectionString = new StringBuilder();
        Properties connectionProps = new Properties();
        connectionProps.put("user", dbUser);
        connectionProps.put("password", dbPassword);
        connectionProps.put("useFetchSizeWithLongColumn", "true");
        connectionProps.put("defaultRowPrefetch", "50");
        try {
            connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append(":").append(dbName);
            System.out.println("Connecting user " + dbUser + " to database " + dbName + " at " + dbHost);
            connection = DriverManager.getConnection(connectionString.toString(), connectionProps);
        } catch (SQLException ex) {
            connectionString.setLength(0);
            connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append("/").append(dbName);
            try {
                System.out.println("Trying again as service name instead of SID. Previous error was: " + ex.getMessage());
                connection = DriverManager.getConnection(connectionString.toString(), connectionProps);
            } catch (SQLException e) {
                throw new UserException("Connection error: " + e.getMessage(), e);
            }
        }
    }

    private static void fetchData() throws UserException {
        System.out.println("Fetching data");
        try {
            final long start = System.nanoTime();
            Statement stmt = connection.createStatement();
            System.out.println("Executing query: " + query);
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsMeta = rs.getMetaData();
            ArrayList<String> header = new ArrayList<>();
            Boolean hasLobs = false;
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                header.add(rsMeta.getColumnName(i));
                if (rsMeta.getColumnTypeName(i) == "CLOB") {
                    hasLobs = true;
                }
            }
            String[] headerArr = new String[header.size()];
            CsvWriter writer = new CsvWriter(OUTPUT_DIR + DATA_OUTPUT_FILE, (includeHeader) ? header.toArray(headerArr) : null);
            // write the result set to csv
            int rowCount = writer.write(rs, hasLobs);
            final long end = System.nanoTime();
            System.out.format("Fetched %d rows in %d seconds%n", rowCount, (end - start) / 1000000000);
            writer.close();
            System.out.println("The output data File was created successfully.");
        } catch (SQLException ex) {
            throw new UserException("SQL Exception: " + ex.getMessage(), ex);
        } catch (CsvException ex) {
            throw new UserException("IO Exception: " + ex.getMessage(), ex);
        }
    }

    private static void getTables() throws UserException {
        System.out.println("Fetching table listing");
        try {
            Statement stmt = connection.createStatement();
            String tableListQuery = "SELECT TABS.TABLE_NAME ,\n" +
                    "    TABS.TABLESPACE_NAME ,\n" +
                    "    TABS.OWNER ,\n" +
                    "    TABS.NUM_ROWS ,\n" +
                    "    COLS.COLUMN_NAME ,\n" +
                    "    COLS.DATA_LENGTH ,\n" +
                    "    COLS.DATA_PRECISION ,\n" +
                    "    COLS.DATA_SCALE ,\n" +
                    "    COLS.COLUMN_ID ,\n" +
                    "    COLS.DATA_TYPE ,\n" +
                    "    COLS.NULLABLE ,\n" +
                    "    REFCOLS.CONSTRAINT_NAME ,\n" +
                    "    REFCOLS.CONSTRAINT_TYPE ,\n" +
                    "    REFCOLS.INDEX_NAME ,\n" +
                    "    REFCOLS.R_CONSTRAINT_NAME,\n" +
                    "    REFCOLS.R_OWNER\n" +
                    "FROM ALL_TAB_COLUMNS COLS\n" +
                    "    JOIN\n" +
                    "    (\n" +
                    "        SELECT \n" +
                    "        TABLE_NAME , \n" +
                    "        TABLESPACE_NAME, \n" +
                    "        OWNER , \n" +
                    "        NUM_ROWS\n" +
                    "        FROM all_tables\n" +
                    "        WHERE all_tables.TABLESPACE_NAME != 'SYSAUX'\n" +
                    "        AND all_tables.TABLESPACE_NAME != 'SYSTEM'\n" +
                    "        AND all_tables.OWNER != 'SYS'\n" +
                    "        AND all_tables.OWNER != 'SYSTEM'\n" +
                    "    )\n" +
                    "    TABS\n" +
                    "        ON COLS.TABLE_NAME = TABS.TABLE_NAME\n" +
                    "        AND COLS.OWNER = TABS.OWNER\n" +
                    "    LEFT OUTER JOIN\n" +
                    "    (\n" +
                    "        SELECT ACC.COLUMN_NAME ,\n" +
                    "        ACC.TABLE_NAME ,\n" +
                    "        AC.CONSTRAINT_NAME ,\n" +
                    "        AC.R_CONSTRAINT_NAME,\n" +
                    "        AC.INDEX_NAME ,\n" +
                    "        AC.CONSTRAINT_TYPE ,\n" +
                    "        AC.R_OWNER\n" +
                    "        FROM ALL_CONS_COLUMNS ACC\n" +
                    "            JOIN ALL_CONSTRAINTS AC\n" +
                    "                ON ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME\n" +
                    "        WHERE AC.CONSTRAINT_TYPE IN ('P', 'U', 'R')\n" +
                    "    )\n" +
                    "    REFCOLS ON COLS.TABLE_NAME = REFCOLS.TABLE_NAME\n" +
                    "        AND COLS.COLUMN_NAME = REFCOLS.COLUMN_NAME";
            ResultSet resultSet = stmt.executeQuery(tableListQuery);
            JSONObject output = new JSONObject();
            while(resultSet.next()) {
                String curTable = resultSet.getString("OWNER") + "." + resultSet.getString("TABLE_NAME");
                if (!output.has(curTable)) {
                    JSONObject tableData = new JSONObject();
                    tableData.put("name", resultSet.getString("TABLE_NAME"));
                    tableData.put("tablespaceName", resultSet.getString("TABLESPACE_NAME"));
                    tableData.put("schema", resultSet.getString("OWNER"));
                    tableData.put("owner", resultSet.getString("OWNER"));
                    if (resultSet.getString("NUM_ROWS") != null) {
                        tableData.put("rowCount", resultSet.getInt("NUM_ROWS"));
                    }
                    output.put(curTable, tableData);
                }

                JSONObject curObject = output.getJSONObject(curTable);
                if (!curObject.has("columns")) {
                    curObject.put("columns", new JSONObject());
                }
                JSONObject curColumns = curObject.getJSONObject("columns");
                String curColumnIndex = String.valueOf(resultSet.getInt("COLUMN_ID") - 1);
                if (!curColumns.has(curColumnIndex)) {
                    JSONObject columnData = new JSONObject();
                    String length = resultSet.getString("DATA_LENGTH");
                    if (resultSet.getString("DATA_PRECISION") != null
                            && resultSet.getString("DATA_SCALE") != null)
                    {
                        length = resultSet.getString("DATA_PRECISION")
                                + "," + resultSet.getString("DATA_SCALE");
                    }
                    columnData.put("name", resultSet.getString("COLUMN_NAME"));
                    columnData.put("type", resultSet.getString("DATA_TYPE"));
                    columnData.put("nullable", resultSet.getString("NULLABLE") == "Y" ? true : false);
                    columnData.put("length", length);
                    columnData.put("ordinalPosition", resultSet.getInt("COLUMN_ID"));
                    columnData.put("primaryKey", false);
                    columnData.put("uniqueKey", false);
                    curColumns.put(curColumnIndex, columnData);
                    curObject.put("columns", curColumns);
                }
                JSONObject currentColumn = curColumns.getJSONObject(curColumnIndex);
                switch (resultSet.getString("CONSTRAINT_TYPE")) {
                    case "R":
                        currentColumn.put("foreignKeyName", resultSet.getString("CONSTRAINT_NAME"));
                        currentColumn.put("foreignKeyTable", resultSet.getString("R_OWNER"));
                        currentColumn.put("foreignKeyRef", resultSet.getString("R_CONSTRAINT_NAME"));
                        break;
                    case "P":
                        currentColumn.put("primaryKey", true);
                        currentColumn.put("primaryKeyName", resultSet.getString("CONSTRAINT_NAME"));
                        break;
                    case "U":
                        currentColumn.put("uniqueKey", true);
                        currentColumn.put("uniqueKeyName", resultSet.getString("CONSTRAINT_NAME"));
                        break;
                    default:
                        break;
                }
                curColumns.put(curColumnIndex, currentColumn);
                curObject.put("columns", curColumns);
                output.put(curTable, curObject);
            }
            try (FileWriter file = new FileWriter(OUTPUT_DIR + TABLES_OUTPUT_FILE)) {
                file.write(output.toString());
                System.out.println("Successfully Copied JSON Table Listing to File...");
            } catch (IOException ioException) {
                throw new UserException("IO Exception: " + ioException.getMessage(), ioException);
            }
        } catch (SQLException sqlException) {
            throw new UserException("SQL Exception: " + sqlException.getMessage(), sqlException);
        }
    }
    
    public static void main(String[] args) {
        try {
            action = args[0];
            System.out.println("executing action " + action);
            readConfigFile(args[1]);
            switch (action) {
                case "testConnection":
                    connectDb();
                    break;
                case "getTables":
                    connectDb();
                    getTables();
                    break;
                default:
                    includeHeader = true;
                    if (args.length > 2) {
                        includeHeader = Boolean.parseBoolean(args[2]);
                    }
                    connectDb();
                    fetchData();
                    break;
            }
            System.out.println("All done");
        } catch (UserException ex) {
            System.err.println(ex.getMessage());
            System.exit(1);
        }catch (ApplicationException ex) {
            System.err.println(ex.getMessage());
            StackTraceElement[] elements = ex.getStackTrace();
            for (int i = 1; i < elements.length; i++) {
                StackTraceElement s = elements[i];
                System.err.println("\tat " + s.getClassName() + "." + s.getMethodName()
                   + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
            }
            System.err.println("Caused by:");
            System.err.println(ex.getCause().getClass());
            System.err.println(ex.getCause().getMessage());
            elements = ex.getCause().getStackTrace();
            for (int i = 1; i < elements.length; i++) {
                StackTraceElement s = elements[i];
                System.err.println("\tat " + s.getClassName() + "." + s.getMethodName()
                   + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
            }
            System.exit(2);
        } finally {
            try {
                connection.close();
            } catch (Throwable e) {}
        }
    }
}
