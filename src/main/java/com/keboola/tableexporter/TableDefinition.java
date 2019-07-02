package com.keboola.tableexporter;

import java.util.HashMap;

public class TableDefinition {

    private String tableName;

    private String schema;

    public TableDefinition(HashMap obj) {
        tableName = obj.get("tableName").toString();
        schema = obj.get("schema").toString();
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchema() {
        return schema;
    }
}
