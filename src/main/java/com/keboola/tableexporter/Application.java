package com.keboola.tableexporter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.CsvException;
import com.keboola.tableexporter.exception.UserException;
import oracle.jdbc.OracleConnection;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class Application {

    private String dbPort;
    private String dbHost;
    private String dbUser;
    private String dbProxyUser;
    private String dbPassword;
    private String dbName;
    private String query;
    private String outputFile;
    private String tnsnamesPath;
    private String tnsnamesService;
    private String defaultRowPrefetch = "50";
    private ArrayList<String> userInitQueries = new ArrayList<String>();
    private static ArrayList<TableDefinition> tables;
    private static OracleConnection connection;
    private static boolean includeHeader;
    private static boolean includeColumns;

    private void readConfigFile(String configFile) throws ApplicationException {
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
        // required params
        dbUser = obj.getJSONObject("parameters").getJSONObject("db").getString("user");
        dbPassword = obj.getJSONObject("parameters").getJSONObject("db").getString("#password");

        // database required if not use tnsnames
        if (tnsnamesPath.equals("")) {
            dbName = obj.getJSONObject("parameters").getJSONObject("db").getString("database");
        }

        // optional params
        if (obj.getJSONObject("parameters").getJSONObject("db").has("port")) {
            dbPort = obj.getJSONObject("parameters").getJSONObject("db").get("port").toString();
        }
        if (obj.getJSONObject("parameters").getJSONObject("db").has("host")) {
            dbHost = obj.getJSONObject("parameters").getJSONObject("db").getString("host");
        }
        if (obj.getJSONObject("parameters").getJSONObject("db").has("proxyUser")) {
            dbProxyUser = obj.getJSONObject("parameters").getJSONObject("db").getString("proxyUser");
        }
        if (obj.getJSONObject("parameters").getJSONObject("db").has("tnsnamesService")) {
            tnsnamesService = obj.getJSONObject("parameters").getJSONObject("db").getString("tnsnamesService");
        }
        if (obj.getJSONObject("parameters").getJSONObject("db").has("defaultRowPrefetch")) {
            defaultRowPrefetch = obj.getJSONObject("parameters").getJSONObject("db").getString("defaultRowPrefetch");
        }
        if (obj.getJSONObject("parameters").has("outputFile")) {
            outputFile = obj.getJSONObject("parameters").getString("outputFile");
        }
        if (obj.getJSONObject("parameters").has("query")) {
            query = obj.getJSONObject("parameters").getString("query");
        }
        tables = new ArrayList<>();
        if (obj.getJSONObject("parameters").has("tables")) {
            List tableList = obj.getJSONObject("parameters").getJSONArray("tables").toList();
            for (int i = 0; i < tableList.size(); i++) {
                tables.add(new TableDefinition((HashMap) tableList.get(i)));
            }
        }
        includeColumns = true;
        if (obj.getJSONObject("parameters").has("includeColumns")) {
            includeColumns = obj.getJSONObject("parameters").getBoolean("includeColumns");
        }

        if (obj.getJSONObject("parameters").getJSONObject("db").has("initQueries")) {
            JSONArray arr = obj.getJSONObject("parameters").getJSONObject("db").getJSONArray("initQueries");
            for (int i = 0; i < arr.length(); i++) {
                userInitQueries.add(arr.getString(i));
            }
        }
    }

    private void connectDb() throws ApplicationException, UserException {
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
        connectionProps.put("defaultRowPrefetch", defaultRowPrefetch);
        connectionProps.put("oracle.net.keepAlive", "true");

        if (tnsnamesPath.equals("")) {
            try {
                connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append(":").append(dbName);
                System.out.println("Connecting user \"" + dbUser + "\" to database \"" + dbName + "\" at \"" + dbHost + "\" on port \"" + dbPort + "\"");
                connection = (OracleConnection) DriverManager.getConnection(connectionString.toString(), connectionProps);
            } catch (SQLException ex) {
                connectionString.setLength(0);
                connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append("/").append(dbName);
                try {
                    System.out.println("Trying again as service name instead of SID. Previous error was: " + ex.getMessage());
                    connection = (OracleConnection) DriverManager.getConnection(connectionString.toString(), connectionProps);
                } catch (SQLException e) {
                    throw new UserException("Connection error: " + e.getMessage(), e);
                }
            }
        } else {
            System.setProperty("oracle.net.tns_admin", tnsnamesPath);
            System.setProperty("oracle.net.keepAlive", "true");
            try {
                connectionString.append("jdbc:oracle:thin:@").append(tnsnamesService);
                System.out.println("Connecting user \"" + dbUser + "\". Using service name \"" + tnsnamesService + "\" from tnsnames.ora.");
                connection = (OracleConnection) DriverManager.getConnection(connectionString.toString(), connectionProps);
            } catch (SQLException ex) {
                throw new UserException("TNSNAMES is probably invalid, please check it, especially SID, ServiceName and parentheses. Connection error: " + ex.getMessage(), ex);
            }
        }

        // Proxy user
        if (dbProxyUser != null) {
            java.util.Properties proxyProp = new java.util.Properties();
            proxyProp.put(OracleConnection.PROXY_USER_NAME, dbProxyUser);
            System.out.println("Proxy user = \"" + dbProxyUser + "\"");

            try {
                connection.openProxySession(OracleConnection.PROXYTYPE_USER_NAME, proxyProp);
            } catch (SQLException ex) {
                throw new UserException("Proxy user error: " + ex.getMessage(), ex);
            }
        }

        for (int i = 0; i < userInitQueries.size(); i++) {
            String query = userInitQueries.get(i);
            System.out.println("Executing query \"" + query + "\"");
            try {
                Statement stmt = connection.createStatement();
                stmt.executeQuery(query);
            } catch (SQLException ex) {
                throw new UserException("SQL Exception: " + ex.getMessage(), ex);
            }
        }
    }

    private void fetchData() throws UserException {
        try {
            final long start = System.nanoTime();
            Statement stmt = connection.createStatement();
            System.out.println("Executing query \"" + query + "\"");
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
            CsvWriter writer = new CsvWriter(outputFile, (includeHeader) ? header.toArray(headerArr) : null);
            // write the result set to csv
            int rowCount = writer.write(rs, hasLobs);
            final long end = System.nanoTime();
            System.out.format("Fetched %d rows in %d seconds%n", rowCount, (end - start) / 1000000000);
            writer.close();
        } catch (SQLException ex) {
            throw new UserException("SQL Exception: " + ex.getMessage(), ex);
        } catch (CsvException ex) {
            throw new UserException("IO Exception: " + ex.getMessage(), ex);
        }
    }

    public void run(String[] args) {
        try {
            String action = args[0];
            tnsnamesPath = "";
            if (args.length > 2) {
                tnsnamesPath = args[2];
            }
            try {
                readConfigFile(args[1]);
            } catch (JSONException ex) {
                throw new UserException(ex.getMessage());
            }
            switch (action) {
                case "testConnection":
                    connectDb();
                    break;
                case "getTables":
                    connectDb();
                    MetaFetcher metaFetcher = new MetaFetcher(connection);
                    ArrayList<TableDefinition> tablesList = tables;
                    TreeMap tables = metaFetcher.fetchTableListing(tablesList, includeColumns);
                    metaFetcher.writeListingToJsonFile(tables, outputFile);
                    break;
                case "export":
                    includeHeader = true;
                    if (args.length > 3) {
                        includeHeader = Boolean.parseBoolean(args[3]);
                    }
                    connectDb();
                    fetchData();
                    break;
                default:
                    throw new UserException("Invalid action provided: '" + action + "' is not supported.");
            }
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

    public static void main(String[] args) {
        Application app = new Application();
        app.run(args);
    }
}
