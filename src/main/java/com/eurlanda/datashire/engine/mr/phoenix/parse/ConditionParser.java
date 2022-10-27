package com.eurlanda.datashire.engine.mr.phoenix.parse;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;

import java.sql.SQLException;

/**
 * Created by zhudebin on 16/2/25.
 */
public class ConditionParser {

    /**
     * 将 表达式转换为 hbase的filter
     * @param condition
     * @return
     * @throws SQLException
     */
    public static Filter parseCondition(String condition) throws SQLException {
        SQLParser parser = new SQLParser(condition, new CustomParseNodeFactory());
        ParseNode parseNode = parser.parseExpression();
        Filter filter = parseNode.accept(new FilterListParseNodeVisitor());
        return filter;
    }

    public static Scan parseConditionToScan(String condition) throws SQLException {
        Scan scan = new Scan();
        scan.setFilter(parseCondition(condition));
        return scan;
    }
}
