package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zhudebin on 2017/5/3.
 */
public class DestSystemHiveSquidTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("test").config("spark.default.parallelism", 2).enableHiveSupport()
                .getOrCreate();

        sparkSession.sessionState().conf().setConfString("hive.exec.dynamic.partition.mode", "nonstrict");

        StructType structType = com.eurlanda.
                datashire.engine.spark.GroupTagTest.genStructType3();

        List<Row> rows = new ArrayList<>();
        int col_id = 1;
        for(int i=0; i<1; i++) {
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*10, "男"}));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*10, "男"}));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*10, "女"}));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*10, "男"}));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*10, "女"}));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*10, "女"}));
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

        TDestSystemHiveSquid tDestSystemHiveSquid = new TDestSystemHiveSquid();
        tDestSystemHiveSquid.preId2Columns = new HashMap<>();
        tDestSystemHiveSquid.squidName = "test";
        tDestSystemHiveSquid.hiveColumns = new ArrayList<>();
        tDestSystemHiveSquid.hiveColumns.add(new Tuple2<String, TStructField>("id", new TStructField("id1", TDataType.INT, true, null, null)));
        tDestSystemHiveSquid.hiveColumns.add(new Tuple2<String, TStructField>("name", new TStructField("name1", TDataType.STRING, true, null, null)));
        tDestSystemHiveSquid.hiveColumns.add(new Tuple2<String, TStructField>("sex", new TStructField("sex1", TDataType.STRING, true, null, null)));
        tDestSystemHiveSquid.hiveColumns.add(new Tuple2<String, TStructField>("age", new TStructField("age1", TDataType.INT, true, null, null)));
        tDestSystemHiveSquid.saveMode = SaveMode.Append;
        tDestSystemHiveSquid.tableName = "lyg.t_user2";
        tDestSystemHiveSquid.preTSquid = preSquid;

        tDestSystemHiveSquid.run(null);



    }

}
