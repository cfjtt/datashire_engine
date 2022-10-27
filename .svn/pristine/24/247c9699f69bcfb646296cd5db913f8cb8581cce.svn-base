package com.eurlanda.datashire.engine.mr.phoenix.parse;

import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Created by zhudebin on 16/2/25.
 */
public class CustomParseNodeFactory extends ParseNodeFactory {

    @Override public ColumnParseNode column(TableName tableName, String name, String alias) {
        return new ColumnParseNode(tableName,caseSensitive(name),alias);
    }

    @Override public TableName table(String schemaName, String tableName) {
        return super.table(caseSensitive(schemaName), caseSensitive(tableName));
    }

    private String caseSensitive(String name) {
        if(name != null && !SchemaUtil.isCaseSensitive(name)) {
            name = "\"" + name + "\"";
        }
        return name;
    }
}
