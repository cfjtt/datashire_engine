package com.eurlanda.datashire.engine.hive;

import com.eurlanda.datashire.engine.spark.DataFrameTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Utils;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Function1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 2017/3/21.
 */
public class HiveSqlTest implements Serializable {

    @Test
    public void test1() {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");



        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        List<Row> list = spark.table("dhparquetdb.tb_text_flight_id_1").alias("AAA")
                .filter("AAA.`flight_id` < '1'")
                .limit(10).collectAsList();
        for(Row r : list) {
            System.out.println(r);
        }

        spark.table("dhparquetdb.tb_text_flight_id_1").printSchema();

//        while(true) {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }

    @Test
    public void test2() {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");



        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        List<Row> list = spark.sql("select * from dhparquetdb.tb_text_flight_id_1 a where flight_id=1").limit(10).collectAsList();


        for(Row r : list) {
            r.schema().toString();
            System.out.println("---------------------------------" + r);
        }

//        spark.table("dhparquetdb.tb_text_flight_id_1").printSchema();

        //        while(true) {
        //            try {
        //                Thread.sleep(1000);
        //            } catch (InterruptedException e) {
        //                e.printStackTrace();
        //            }
        //        }
    }

    @Test
    public void test3() {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");



        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        List<Row> list = spark.table("dhparquetdb.tb_text_flight_id_1").alias("t1").filter("t1.a_s_nws_fault!='ON'").limit(10).collectAsList();


        for(Row r : list) {
            System.out.println(r.schema().toString());
            System.out.println("---------------------------------" + r);
        }
    }

    @Test
    public void test4() {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");



        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        List<Row> list = spark.table("dhparquetdb.spark10000").limit(1).collectAsList();


        for(Row r : list) {
            System.out.println(r.schema().toString());
            System.out.println("---------------------------------" + r);
        }
    }

    /**
     * 测试hive落地
     */
    @Test
    public void test5() {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test hive");



        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        spark.sessionState().conf().setConfString("hive.exec.dynamic.partition.mode", "nonstrict");

//        Dataset<Row> ds = spark.table("lyg.test2").limit(10);

        List<Long> list = new ArrayList<>();
        for(long l=0; l<100; l++) {
            list.add(l);
        }
        RDD<Row> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(list).map(
                new Function<Long, Row>() {
                    @Override public Row call(Long v1) throws Exception {
                        return new GenericRow(new Object[]{"hp_" + v1, "xsfx_" + v1%4, "kkbh_" + v1%20});
                    }
                }).rdd();

        Dataset<Row> ds = spark.createDataFrame(rdd, DataFrameTest.genStructType());

//        ds = ds.withColumn("extractionDate", Utils.genColumn("NOW()"));
        ds = ds.withColumn("extractionDate", org.apache.spark.sql.functions.current_timestamp());


        System.out.println("--------------");
        ds.printSchema();
        List<Row> rows = ds.limit(10).collectAsList();
        for(Row r : rows) {
            System.out.println("-------" + r);
        }
        System.out.println("--------------");

        ds.write().mode(SaveMode.Overwrite).insertInto("lyg.test2");



    }

    /**
     * 测试Hive原生接口
     */
    @Test
    public void test6() throws Exception {
        HiveConf hiveConf = new HiveConf();
//        HiveClientCache cache = new HiveClientCache(1000);
        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        Table table = client.getTable("default", "t_user1");
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        List<FieldSchema> cols = table.getSd().getCols();
        System.out.println(table);
    }
}
