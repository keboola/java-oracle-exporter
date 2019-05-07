package com.keboola.tableexporter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.keboola.tableexporter.exception.UserException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MetaFetcher {

    private String outputFile;

    private Connection connection;

    public MetaFetcher(Connection connection, String outputFile) {
        this.outputFile = outputFile;
        this.connection = connection;
    }

    public void getTables() throws UserException {
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
            TreeMap output = new TreeMap();
            while(resultSet.next()) {
                String curTable = resultSet.getString("OWNER") + "." + resultSet.getString("TABLE_NAME");
                if (!output.containsKey(curTable)) {
                    LinkedHashMap tableData = new LinkedHashMap();
                    tableData.put("name", resultSet.getString("TABLE_NAME"));
                    tableData.put("tablespaceName", resultSet.getString("TABLESPACE_NAME"));
                    tableData.put("schema", resultSet.getString("OWNER"));
                    tableData.put("owner", resultSet.getString("OWNER"));
                    if (resultSet.getString("NUM_ROWS") != null) {
                        tableData.put("rowCount", resultSet.getInt("NUM_ROWS"));
                    }
                    output.put(curTable, tableData);
                }

                LinkedHashMap curObject = new LinkedHashMap((HashMap) output.get(curTable));
                if (!curObject.containsKey("columns")) {
                    curObject.put("columns", new TreeMap<Integer, JSONObject>());
                }
                TreeMap curColumns = new TreeMap((TreeMap) curObject.get("columns"));

                Integer curColumnIndex = resultSet.getInt("COLUMN_ID") - 1;
                if (!curColumns.containsKey(curColumnIndex)) {
                    LinkedHashMap columnData = new LinkedHashMap();
                    String length = resultSet.getString("DATA_LENGTH");
                    if (resultSet.getString("DATA_PRECISION") != null
                            && resultSet.getString("DATA_SCALE") != null)
                    {
                        length = resultSet.getString("DATA_PRECISION")
                                + "," + resultSet.getString("DATA_SCALE");
                    }
                    columnData.put("name", resultSet.getString("COLUMN_NAME"));
                    columnData.put("sanitizedName", columnNameSanitizer(resultSet.getString("COLUMN_NAME")));
                    columnData.put("type", resultSet.getString("DATA_TYPE"));
                    if (resultSet.getString("NULLABLE").equals("Y")) {
                        columnData.put("nullable",  true);
                    } else {
                        columnData.put("nullable",  false);
                    }
                    columnData.put("length", length);
                    columnData.put("ordinalPosition", resultSet.getInt("COLUMN_ID"));
                    columnData.put("primaryKey", false);
                    columnData.put("uniqueKey", false);
                    curColumns.put(curColumnIndex, columnData);
                    curObject.put("columns", curColumns);
                }
                LinkedHashMap currentColumn = (LinkedHashMap) curColumns.get(curColumnIndex);
                if (resultSet.getString("CONSTRAINT_TYPE") != null) {
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
                }
                curColumns.put(curColumnIndex, currentColumn);
                curObject.put("columns", curColumns);
                output.put(curTable, curObject);
            }
            try (FileWriter file = new FileWriter(outputFile)) {
                JSONArray outputArray = new JSONArray();
                Iterator it = output.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry table = (Map.Entry)it.next();
                    JSONArray columnsArray = new JSONArray();
                    LinkedHashMap tableMap = new LinkedHashMap((LinkedHashMap) table.getValue());
                    TreeMap<Integer, JSONObject> columns = (TreeMap<Integer, JSONObject>) tableMap.get("columns");
                    Iterator columnsIterator = columns.entrySet().iterator();
                    while (columnsIterator.hasNext()) {
                        Map.Entry column = (Map.Entry)columnsIterator.next();
                        columnsArray.add(column.getValue());
                        columnsIterator.remove();
                    }
                    tableMap.put("columns", columnsArray);
                    outputArray.add(tableMap);
                    it.remove();
                }
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                file.write(gson.toJson(outputArray));
                System.out.println("Successfully Copied JSON Table Listing to File " + outputFile);
            } catch (IOException ioException) {
                throw new UserException("IO Exception: " + ioException.getMessage(), ioException);
            }
        } catch (SQLException sqlException) {
            throw new UserException("SQL Exception: " + sqlException.getMessage(), sqlException);
        }
    }

    public static String columnNameSanitizer(String columnName)
    {
        ArrayList<String> sysColumns = new ArrayList<>(Arrays.asList(
                "oid",
                "tableoid",
                "xmin",
                "cmin",
                "xmax",
                "cmax",
                "ctid"
        ));

        String replaced = columnName.replaceAll("[^A-Za-z0-9_]", "_");
        replaced = replaced.replaceFirst("^_", "");
        if (sysColumns.contains(replaced.toLowerCase())) {
            return replaced + "_";
        }
        return replaced;
    }
}
