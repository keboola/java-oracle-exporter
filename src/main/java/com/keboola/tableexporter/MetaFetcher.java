package com.keboola.tableexporter;

import com.keboola.tableexporter.exception.UserException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class MetaFetcher {

    private Connection connection;

    public MetaFetcher(Connection connection) {
        this.connection = connection;
    }

    public TreeMap fetchTableListing(ArrayList<TableDefinition> tables, boolean includeColumns) throws UserException {
        System.out.println("Fetching table listing");
        try {
            PreparedStatement stmt = includeColumns ? tableListingQuery(tables) : onlyTablesQuery(tables);
            ResultSet resultSet = stmt.executeQuery();
            TreeMap output = new TreeMap();
            while(resultSet.next()) {
                String curTable = resultSet.getString("OWNER") + "." + resultSet.getString("TABLE_NAME");
                if (!output.containsKey(curTable)) {
                    LinkedHashMap tableData = getTableData(resultSet);
                    output.put(curTable, tableData);
                }
                if (!includeColumns) {
                    continue;
                }

                LinkedHashMap curObject = new LinkedHashMap((HashMap) output.get(curTable));
                if (!curObject.containsKey("columns")) {
                    curObject.put("columns", new TreeMap<Integer, JSONObject>());
                }
                TreeMap curColumns = new TreeMap((TreeMap) curObject.get("columns"));

                Integer curColumnIndex = resultSet.getInt("COLUMN_ID") - 1;
                if (!curColumns.containsKey(curColumnIndex)) {
                    LinkedHashMap columnData = getColumnData(resultSet);
                    curColumns.put(curColumnIndex, columnData);
                    curObject.put("columns", curColumns);
                }
                LinkedHashMap currentColumn = (LinkedHashMap) curColumns.get(curColumnIndex);
                if (resultSet.getString("CONSTRAINT_TYPE") != null) {
                    switch (resultSet.getString("CONSTRAINT_TYPE")) {
                        case "R":
                            currentColumn.put("foreignKeyName", resultSet.getString("CONSTRAINT_NAME"));
                            currentColumn.put("foreignKeyRefTable", resultSet.getString("R_OWNER"));
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
            return output;
        } catch (SQLException sqlException) {
            throw new UserException("SQL Exception: " + sqlException.getMessage(), sqlException);
        }
    }

    public LinkedHashMap getTableData(ResultSet rs) throws SQLException {
        LinkedHashMap tableData = new LinkedHashMap();
        tableData.put("name", rs.getString("TABLE_NAME"));
        if (rs.getString("TABLESPACE_NAME") != null) {
            tableData.put("tablespaceName", rs.getString("TABLESPACE_NAME"));
        }
        tableData.put("schema", rs.getString("OWNER"));
        tableData.put("owner", rs.getString("OWNER"));
        if (rs.getString("NUM_ROWS") != null) {
            tableData.put("rowCount", rs.getInt("NUM_ROWS"));
        }
        return tableData;
    }

    public LinkedHashMap getColumnData(ResultSet rs) throws SQLException {
        LinkedHashMap columnData = new LinkedHashMap();
        columnData.put("name", rs.getString("COLUMN_NAME"));
        columnData.put("sanitizedName", columnNameSanitizer(rs.getString("COLUMN_NAME")));
        columnData.put("type", rs.getString("DATA_TYPE"));
        if (rs.getString("NULLABLE").equals("Y")) {
            columnData.put("nullable",  true);
        } else {
            columnData.put("nullable",  false);
        }
        columnData.put("length", getColumnLength(rs));
        columnData.put("ordinalPosition", rs.getInt("COLUMN_ID"));
        columnData.put("primaryKey", false);
        columnData.put("uniqueKey", false);
        return columnData;
    }

    public String getColumnLength(ResultSet resultSet) throws SQLException
    {
        String[] charTypes = {"CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"};
        if (resultSet.getString("DATA_PRECISION") != null) {
            String length = resultSet.getString("DATA_PRECISION");
            if (resultSet.getString("DATA_SCALE") != null) {
                length += "," + resultSet.getString("DATA_SCALE");
            }
            return length;
        } else if (Arrays.asList(charTypes).contains(resultSet.getString("DATA_TYPE"))) {
            return resultSet.getString("CHAR_LENGTH");
        }
        return null;
    }

    public void writeListingToJsonFile(TreeMap output, String outputFile) throws UserException
    {
        try (FileWriter file = new FileWriter(outputFile)) {
            JSONArray outputArray = new JSONArray();
            Iterator it = output.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry table = (Map.Entry)it.next();
                JSONArray columnsArray = new JSONArray();
                LinkedHashMap tableMap = new LinkedHashMap((LinkedHashMap) table.getValue());
                if (tableMap.containsKey("columns")) {
                    TreeMap<Integer, JSONObject> columns = (TreeMap<Integer, JSONObject>) tableMap.get("columns");
                    Iterator columnsIterator = columns.entrySet().iterator();
                    while (columnsIterator.hasNext()) {
                        Map.Entry column = (Map.Entry)columnsIterator.next();
                        columnsArray.add(column.getValue());
                        columnsIterator.remove();
                    }
                    tableMap.put("columns", columnsArray);
                }
                outputArray.add(tableMap);
                it.remove();
            }
            file.write(outputArray.toJSONString());
            System.out.println("Successfully Copied JSON Table Listing to File " + outputFile);
        } catch (IOException ioException) {
            throw new UserException("IO Exception: " + ioException.getMessage(), ioException);
        }
    }

    private PreparedStatement onlyTablesQuery(ArrayList<TableDefinition> tables) throws SQLException {
        String sql = "SELECT \n" +
                "        TABLE_NAME , \n" +
                "        TABLESPACE_NAME, \n" +
                "        OWNER, \n" +
                "        NUM_ROWS\n" +
                "        FROM all_tables\n" +
                "        WHERE all_tables.TABLESPACE_NAME != 'SYSAUX'\n" +
                "        AND all_tables.TABLESPACE_NAME != 'SYSTEM'\n" +
                "        AND all_tables.OWNER != 'SYS'\n" +
                "        AND all_tables.OWNER != 'SYSTEM'\n" +
                "        UNION ALL\n" +
                "        SELECT VIEW_NAME, '', OWNER, 0 FROM ALL_VIEWS \n" +
                "        WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'DMSYS', 'CTXSYS', 'XDB', 'APEX_040000')\n" +
                "        UNION ALL\n" +
                "        SELECT TABLE_NAME, '', TABLE_OWNER, 0 FROM USER_SYNONYMS \n" +
                "        WHERE TABLE_OWNER != 'SYS' AND TABLE_OWNER != 'SYSTEM'\n";

        ArrayList<String> statementValues = new ArrayList<>();
        if (tables.size() > 0) {
            String whereClause = "\nWHERE ";
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) {
                    whereClause += " OR ";
                }
                whereClause += "(";
                whereClause += "TABS.TABLE_NAME = ? AND ";
                whereClause += "TABS.OWNER = ?";
                whereClause += ")\n";
                statementValues.add(tables.get(i).getTableName());
                statementValues.add(tables.get(i).getSchema());
            }
            sql = "SELECT * FROM (" + sql + ") TABS " + whereClause;
        }
        PreparedStatement stmt = connection.prepareStatement(sql);
        for (int i = 0; i < statementValues.size(); i++) {
            stmt.setString(i + 1, statementValues.get(i));
        }
        return stmt;
    }

    private PreparedStatement tableListingQuery(ArrayList<TableDefinition> tables) throws SQLException {
        String sql = "SELECT TABS.TABLE_NAME ,\n" +
                "    TABS.TABLESPACE_NAME ,\n" +
                "    TABS.OWNER ,\n" +
                "    TABS.NUM_ROWS ,\n" +
                "    COLS.COLUMN_NAME ,\n" +
                "    COLS.CHAR_LENGTH ,\n" +
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
                "        UNION ALL\n" +
                "        SELECT VIEW_NAME, '', OWNER, 0 FROM ALL_VIEWS \n" +
                "        WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'MDSYS', 'DMSYS', 'CTXSYS', 'XDB', 'APEX_040000')\n" +
                "        UNION ALL\n" +
                "        SELECT TABLE_NAME, '', TABLE_OWNER, 0 FROM USER_SYNONYMS \n" +
                "        WHERE TABLE_OWNER != 'SYS' AND TABLE_OWNER != 'SYSTEM'\n" +
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

        ArrayList<String> statementValues = new ArrayList<>();
        if (tables.size() > 0) {
            sql += "\nWHERE ";
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) {
                    sql += " OR ";
                }
                sql += "(";
                sql += "TABS.TABLE_NAME = ? AND ";
                sql += "TABS.OWNER = ?";
                sql += ")\n";
                statementValues.add(tables.get(i).getTableName());
                statementValues.add(tables.get(i).getSchema());
            }
        }
        PreparedStatement stmt = connection.prepareStatement(sql);
        for (int i = 0; i < statementValues.size(); i++) {
            stmt.setString(i + 1, statementValues.get(i));
        }
        return stmt;
    }

    public static String columnNameSanitizer(String columnName) {
        ArrayList<String> sysColumns = new ArrayList<>(Arrays.asList(
                "oid",
                "tableoid",
                "xmin",
                "cmin",
                "xmax",
                "cmax",
                "ctid"
        ));

        String replaced = columnName.replaceAll("[^A-Za-z0-9_]+", "_");
        replaced = replaced.replaceFirst("^_", "");
        if (sysColumns.contains(replaced.toLowerCase())) {
            return replaced + "_";
        }
        return replaced;
    }
}
