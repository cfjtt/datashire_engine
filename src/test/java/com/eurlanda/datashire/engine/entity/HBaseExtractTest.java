package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.squidFlow.AbstractSquidTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 16/2/22.
 */
public class HBaseExtractTest extends AbstractSquidTest {

    @Test
    public void test1() {
        TSquidFlow tSquidFlow = new TSquidFlow();
        tSquidFlow.setDebugModel(false);

        THBaseSquid thBaseSquid = new THBaseSquid();
        thBaseSquid.setTableName("testtable");
        thBaseSquid.setUrl("e101:2181");

        List<TColumn> columns = new ArrayList<>();
        columns.add(new TColumn(TDataType.VARBINARY, ConstantsUtil.HBASE_ROW_KEY, 1));
        columns.add(new TColumn(TDataType.VARBINARY, "cf1:name", 2));
        columns.add(new TColumn(TDataType.VARBINARY, "cf2:addr", 2));

        thBaseSquid.setColumns(columns);

        thBaseSquid.run(sc);

        JavaRDD<Map<Integer, DataCell>> outRDD = thBaseSquid.getOutRDD();
        printRDD(outRDD);

    }

    @Test
    public void test2() {
        TSquidFlow tSquidFlow = new TSquidFlow();
        tSquidFlow.setDebugModel(false);
        THBaseSquid thBaseSquid = new THBaseSquid();
        thBaseSquid.setTableName("testtable");
        thBaseSquid.setUrl("e101:2181");

        List<TColumn> columns = new ArrayList<>();
        columns.add(new TColumn(TDataType.VARBINARY, ConstantsUtil.HBASE_ROW_KEY, 1));
        columns.add(new TColumn(TDataType.VARBINARY, "cf1:name", 2));
        columns.add(new TColumn(TDataType.VARBINARY, "cf2:addr", 2));

        thBaseSquid.setColumns(columns);

        tSquidFlow.addSquid(thBaseSquid);


    }

    @Test
    public void test3() {
    }

    private static void printRDD(JavaRDD<Map<Integer, DataCell>> outRDD) {
        List<Map<Integer, DataCell>> list = outRDD.collect();

        long count = list.size();
        System.out.println("------------" + count + "------------");
        for(Map<Integer, DataCell> m : list) {
            printMap(m);
        }
    }

    @Test
    public void initData() throws IOException {
        // vv CRUDExample
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "e101");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // vv CRUDExample
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        //
        connection.getAdmin().listTableNames();

        for(int i=3; i<1000; i++) {
            Put put2 = new Put(Bytes.toBytes(i));
            put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes("name-0" + i));
            put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"),
                    Bytes.toBytes((20 + i)%90));
            put2.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("addr"),
                    Bytes.toBytes("addr-0" + i));
            table.put(put2);
        }

        table.close();
    }
}
