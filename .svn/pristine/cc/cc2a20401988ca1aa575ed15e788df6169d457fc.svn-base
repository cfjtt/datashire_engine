package com.eurlanda.datashire.engine.entity;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 2017/3/22.
 */
public class THiveSquidTest {

    private static SparkSession genSession() {
//        SparkSession sparkSession = SparkSession.builder()
//                .master("local[1]").enableHiveSupport()
//                .appName("test").config("spark.default.parallelism", 2)
//                .getOrCreate();
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");
        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        return spark;
    }

    @Test
    public void test1() {
        String tableName = "dhparquetdb.tb_parquet_flight_id_1";
        String alias = "t1";
        String filter = "t1.flight_id != 1";
        List<TColumn> columns = new ArrayList<>();
        // ac_tail1, ac_tail123,flight_id
        columns.add(new TColumn(1, "c1", "ac_tail1", TDataType.STRING, true));
        columns.add(new TColumn(2, "c2", "ac_tail123", TDataType.STRING, true));
        columns.add(new TColumn(3, "c3", "flight_id", TDataType.STRING, true));

        SparkSession session = genSession();
        THiveSquid tHiveSquid = new THiveSquid();
        TJobContext jc = new TJobContext();
        jc.setSparkSession(session);
        tHiveSquid.setJobContext(jc);
        tHiveSquid.setAlias(alias);
        tHiveSquid.setTableName(tableName);
        tHiveSquid.setColumns(columns);
        tHiveSquid.setFilter(filter);

        tHiveSquid.run(null);
        Dataset<Row> ds = tHiveSquid.outDataFrame;
        List<Row> list = ds
                .limit(10)
                .collectAsList();
        System.out.println("------------------------");
        ds.printSchema();
        for(Row r : list) {
            System.out.println(r);
        }
        System.out.println("------------------------");
    }


    @Test
    public void test2() {
        String tableName = "dhparquetdb.tb_parquet_flight_id_1";
        String alias = "t1";
        String filter = "t1.flight_id = 1";
        List<TColumn> columns = new ArrayList<>();
        // ac_tail1, ac_tail123,flight_id
        columns.add(new TColumn(1, "c1", "ac_tail1", TDataType.STRING, true));
        columns.add(new TColumn(2, "c2", "ac_tail123", TDataType.STRING, true));
        columns.add(new TColumn(3, "c3", "flight_id", TDataType.STRING, true));

        SparkSession session = genSession();
        THiveSquid tHiveSquid = new THiveSquid();
        TJobContext jc = new TJobContext();
        jc.setSparkSession(session);
        tHiveSquid.setJobContext(jc);
        tHiveSquid.setAlias(alias);
        tHiveSquid.setTableName(tableName);
        tHiveSquid.setColumns(columns);
        tHiveSquid.setFilter(filter);

        tHiveSquid.run(null);
        List<Map<Integer, DataCell>> list = tHiveSquid.outRDD.take(10);
        System.out.println("------------------------");
        for(Map<Integer, DataCell> r : list) {
            for(Map.Entry e : r.entrySet()) {
                System.out.println(e.getKey() + "----------------" + e.getValue());
            }
        }
        System.out.println("------------------------");
    }
}
