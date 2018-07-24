package com.keboola.tableexporter;

import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;

import com.keboola.tableexporter.exception.ApplicationException;
import com.keboola.tableexporter.exception.CsvException;
import com.keboola.tableexporter.exception.UserException;
import org.json.JSONArray;
import org.json.JSONObject;

public class Application {
    private static String dbPort;
    private static String dbHost;
    private static String dbUser;
    private static String dbPassword;
    private static String dbName;
    private static String query;
    private static String outputFile;
    private static String outputTable;
    private static String table;
    private static String schema;
    private static boolean incremental;
    private static JSONArray primaryKey;
    private static Connection connection;
    
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
        JSONObject params = obj.getJSONObject("parameters");
        JSONObject db = params.getJSONObject("db");
        dbPort = db.getString("port");
        dbHost = db.getString("host");
        dbUser = db.getString("user");
        dbPassword = db.getString("#password");
        dbName = db.getString("database");
        query = params.getString("query");
        outputFile = params.getString("outputFile");
        outputTable = params.getString("outputTable");
        if (params.has("incremental")) {
            incremental = params.getBoolean("incremental");
        }
        if (params.has("primaryKey")) {
            primaryKey = params.getJSONArray("primaryKey");
        }
        if (params.has("table")) {
            table = params.getJSONObject("table").getString("tableName");
            schema = params.getJSONObject("table").getString("schema");
        }
    }    
    
    private static void connectDb() throws ApplicationException, UserException {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException ex) {
            throw new ApplicationException("Driver error", ex);
        }
        StringBuilder connectionString = new StringBuilder();
        try {
            connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append(":").append(dbName);
            System.out.println("Connecting user " + dbUser + " to database " + dbName + " at " + dbHost);
            connection = DriverManager.getConnection(connectionString.toString(), dbUser, dbPassword);
        } catch (SQLException ex) {
            connectionString.setLength(0);
            connectionString.append("jdbc:oracle:thin:@").append(dbHost).append(":").append(dbPort).append("/").append(dbName);
            try {
                System.out.println("Trying again as service name instead of SID. Previous error was: " + ex.getMessage());
                connection = DriverManager.getConnection(connectionString.toString(), dbUser, dbPassword);
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
            DatabaseMetaData dbMeta = connection.getMetaData();
            System.out.println("Executing query: " + query);
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsMeta = rs.getMetaData();
            Boolean hasLobs = false;
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                if (rsMeta.getColumnTypeName(i) == "CLOB") {
                    hasLobs = true;
                }
            }
            CsvWriter writer = new CsvWriter(outputFile, null);
            // write the result set to csv
            int rowCount = writer.write(rs, hasLobs);
            final long end = System.nanoTime();
            System.out.format("Fetched %d rows in %d seconds%n", rowCount, (end - start) / 1000000000);
            writer.close();
            System.out.println("Data File " + outputFile + " was created successfully.");
            writeManifest(outputFile, rsMeta, dbMeta);
        } catch (SQLException ex) {
            throw new UserException("SQL Exception: " + ex.getMessage(), ex);
        } catch (CsvException ex) {
            throw new UserException("IO Exception: " + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new UserException("IO Exception: " + ex.getMessage(), ex);
        }
    }

    private static void writeManifest(
        String outputFile,
        ResultSetMetaData rsMeta,
        DatabaseMetaData dbMeta
    ) throws UserException, IOException {
        try {
            JSONArray columnNames = new JSONArray();
            JSONArray tableMetadata = new JSONArray();
            JSONObject manifest = new JSONObject();
            JSONObject allColumnMetadata = new JSONObject();
            ArrayList<String> pkList = new ArrayList<String>();
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                // just get table metadata for the first column (this may result in bs results for complicated queries)
                if (i == 1) {
                    String catalog = rsMeta.getCatalogName(i);
                    String schema = rsMeta.getSchemaName(i);
                    String table = rsMeta.getTableName(i);
                    System.out.println("CATALOG " + catalog + " SCHEMA " + schema + " TABLE " + table);
                    ResultSet pks = dbMeta.getPrimaryKeys(catalog, schema, table);
                    tableMetadata.put(makeMetadataObject("name", table));
                    tableMetadata.put(makeMetadataObject("schema", schema));
                    tableMetadata.put(makeMetadataObject("catalog", catalog));
                    while (pks.next()) {
                        System.out.println("ADDING PK " + pks.getString("COLUMN_NAME"));
                        pkList.add(pks.getString("COLUMN_NAME"));
                    }
                }

                JSONArray columnMetadata = new JSONArray();
                String name = rsMeta.getColumnName(i);
                columnNames.put(name);
                columnMetadata.put(makeMetadataObject("name", name));
                columnMetadata.put(makeMetadataObject("ordinalPosition", String.valueOf(i)));
                columnMetadata.put(makeMetadataObject("autoIncrement", String.valueOf(rsMeta.isAutoIncrement(i))));
                columnMetadata.put(makeMetadataObject("primaryKey", String.valueOf(pkList.contains(name))));
                columnMetadata.put(makeMetadataObject("isCurrency", String.valueOf(rsMeta.isCurrency(i))));
                columnMetadata.put(makeMetadataObject("datatype.type", rsMeta.getColumnTypeName(i)));
                String length = rsMeta.getPrecision(i) != 0 ? String.valueOf(rsMeta.getPrecision(i)) : "null";
                if (rsMeta.getScale(i) != 0) {
                    length += "," + rsMeta.getScale(i);
                }
                columnMetadata.put(makeMetadataObject("datatype.length", length));
                columnMetadata.put(makeMetadataObject("datatype.nullable", String.valueOf(rsMeta.isNullable(i) == 0)));
                allColumnMetadata.put(name, columnMetadata);
            }

            manifest.put("columns", columnNames);
            manifest.put("metadata", tableMetadata);
            manifest.put("columnMetadata", allColumnMetadata);
            manifest.put("destination", outputTable);
            manifest.put("incremental", incremental);
            manifest.put("primaryKey", primaryKey);
            try (FileWriter file = new FileWriter(outputFile + ".manifest")) {
                file.write(manifest.toString());
                System.out.println("Successfully Copied JSON Object to File...");
                System.out.println("\nJSON Object: " + manifest.toString());
            }
        } catch (SQLException e) {
            throw new UserException("Error writing manifest: " + e.getMessage(), e);
        }

    }

    private static JSONObject makeMetadataObject(String key, String value) {
        JSONObject output = new JSONObject();
        output.put("key", "KBC." + key);
        output.put("value", value);
        return output;
    }
    
    public static void main(String[] args) {
        try {
            readConfigFile(args[0]);
            connectDb();
            fetchData();
            System.out.println("All done");
        } catch (ApplicationException ex) {
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
        } catch (UserException ex) {
            System.err.println(ex.getMessage());
            System.exit(1);
        }
    }
}
