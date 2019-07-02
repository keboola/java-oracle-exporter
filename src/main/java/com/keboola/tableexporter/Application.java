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
import org.json.JSONObject;

public class Application {

    private static String dbPort;
    private static String dbHost;
    private static String dbUser;
    private static String dbPassword;
    private static String dbName;
    private static String query;
    private static String outputFile;
    private static ArrayList<TableDefinition> tables;
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
            CsvWriter writer = new CsvWriter(outputFile, (includeHeader) ? header.toArray(headerArr) : null);
            // write the result set to csv
            int rowCount = writer.write(rs, hasLobs);
            final long end = System.nanoTime();
            System.out.format("Fetched %d rows in %d seconds%n", rowCount, (end - start) / 1000000000);
            writer.close();
            System.out.println("The output data file was created successfully.");
        } catch (SQLException ex) {
            throw new UserException("SQL Exception: " + ex.getMessage(), ex);
        } catch (CsvException ex) {
            throw new UserException("IO Exception: " + ex.getMessage(), ex);
        }
    }
    
    public static void main(String[] args) {
        try {
            String action = args[0];
            System.out.println("executing action " + action);
            readConfigFile(args[1]);
            switch (action) {
                case "testConnection":
                    connectDb();
                    break;
                case "getTables":
                    connectDb();
                    MetaFetcher metaFetcher = new MetaFetcher(connection);
                    ArrayList<TableDefinition> tablesList = tables;
                    TreeMap tables = metaFetcher.fetchTableListing(tablesList);
                    metaFetcher.writeListingToJsonFile(tables, outputFile);
                    break;
                case "export":
                    includeHeader = true;
                    if (args.length > 2) {
                        includeHeader = Boolean.parseBoolean(args[2]);
                    }
                    connectDb();
                    fetchData();
                    break;
                default:
                    throw new UserException("Invalid action provided: '" + action + "' is not supported.");
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
