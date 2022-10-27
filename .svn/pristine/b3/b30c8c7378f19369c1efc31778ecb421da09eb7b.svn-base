package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.mr.phoenix.parse.CustomParseNodeFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Created by zhudebin on 16/2/26.
 */
public class ConditionParserTest {

    @Test
    public void test1() throws SQLException {
        String condition = "a.b > c and (c.d<10 or a.d =100) and c.a =10 ";
        SQLParser parser = new SQLParser(condition, new CustomParseNodeFactory());
        ParseNode parseNode = parser.parseExpression();
        System.out.println(parseNode);

        short s = 1;
        byte[] bs = Bytes.toBytes(s);
        System.out.println(Bytes.toShort(bs));
//        System.out.println(Bytes.toInt(bs));
//        System.out.println(Bytes.toLong(bs));

    }

}
