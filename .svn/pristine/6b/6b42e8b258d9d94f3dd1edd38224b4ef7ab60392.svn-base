package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhudebin on 2017/3/8.
 */
public class TestTGroupTagSquid {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("test").config("spark.default.parallelism", 2)
                .getOrCreate();

        //        StructField("id", IntegerType, nullable = true),
        //                StructField("name", StringType, nullable = true),
        //                StructField("age", IntegerType, nullable = true),
        //                StructField("sex", StringType, nullable = true))
        StructType structType = com.eurlanda.
                datashire.engine.spark.GroupTagTest.genStructType2();

        List<Row> rows = new ArrayList<>();
        int col_id = 1;
        for(int i=0; i<1; i++) {
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*1000, "男", "tag_origin", "tag_origin1" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*1000, "男", "tag_origin", "tag_origin1" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*1000, "女", "tag_origin", "tag_origin1" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*1000, "男", "tag_origin", "tag_origin1" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*1000, "女", "tag_origin", "tag_origin1" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*1000, "女", "tag_origin", "tag_origin1" }));
        }
        Dataset<Row> ds = sparkSession.createDataFrame(rows, structType);

        for(StructField sf : ds.schema().fields()) {
            System.out.println("++++++" + sf);
        }

        TSquid preSquid = new TSquid(){
            @Override protected Object run(JavaSparkContext jsc) throws EngineException {
                return null;
            }
        };
        preSquid.outDataFrame = ds;

        TGroupTagSquid tGroupTagSquid = new TGroupTagSquid();
        tGroupTagSquid.preId2Columns = new HashMap<>();
        tGroupTagSquid.groupColumns = Lists.newArrayList("age");
        tGroupTagSquid.sortColumns = Lists.newArrayList(new TOrderItem("id", false));
        tGroupTagSquid.tagGroupColumns = Lists.newArrayList("sex");
        tGroupTagSquid.squidName = "test";
        tGroupTagSquid.preTSquid = preSquid;

        tGroupTagSquid.run(null);

        StructType st1 = tGroupTagSquid.outDataFrame.schema();
        for(StructField sf : st1.fields()) {
            System.out.println("--------" + sf);
        }

        List<Row> list = tGroupTagSquid.outDataFrame.select("id", "age").collectAsList();
        for(Row r : list) {
            System.out.println(r);
        }

        list = tGroupTagSquid.outDataFrame.collectAsList();
        for(Row r : list) {
            System.out.println(r);
        }


    }


}
