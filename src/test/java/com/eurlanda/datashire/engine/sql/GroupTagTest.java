package com.eurlanda.datashire.engine.sql;

import org.apache.spark.Partition;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by zhudebin on 2017/3/8.
 */
public class GroupTagTest {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("test").config("spark.default.parallelism", 2)
                .getOrCreate();

//        StructField("id", IntegerType, nullable = true),
//                StructField("name", StringType, nullable = true),
//                StructField("age", IntegerType, nullable = true),
//                StructField("sex", StringType, nullable = true))
        StructType structType = com.eurlanda.datashire.engine.spark.GroupTagTest.genStructType();

        List<Row> rows = new ArrayList<>();
        int col_id = 1;
        for(int i=0; i<10; i++) {
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*1000, "男" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*1000, "男" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*1000, "女" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 22+i*1000, "男" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 21+i*1000, "女" }));
            rows.add(new GenericRow(new Object[] { col_id++, "name_" + col_id, 20+i*1000, "女" }));
        }
        Dataset<Row> ds = sparkSession.createDataFrame(rows, structType);

        // 添加一列
//        ds.withColumn()

        StructType ssst = ds.withColumnRenamed("id", "id2").schema();
        for(StructField sf : ssst.fields()) {
            System.out.println(sf);
        }

        Dataset<Row> reDS = ds.repartition(1);
        List<Partition> partitions = reDS.toJavaRDD().partitions();
        System.out.println(partitions.size());

        Encoder en = null;
//        com.eurlanda.datashire.engine.spark.GroupTagTest.genExpression(
//                com.eurlanda.datashire.engine.spark.GroupTagTest.genStructType1_2()
//        );

        en = com.eurlanda.datashire.engine.spark.GroupTagTest.genExpression2();

        Dataset<Row> tagDS = ds.repartition(new Column("age"))
                .sortWithinPartitions(new Column("id").desc()).mapPartitions(
                new MapPartitionsFunction<Row, Row>() {

                    @Override public Iterator<Row> call(final Iterator<Row> input) throws Exception {

                        return new Iterator<Row>() {
                            int key = new Random().nextInt(100);
                            Object last = null;
                            int index = 0;
                            StructType st = null;
                            @Override public boolean hasNext() {
                                return input.hasNext();
                            }

                            @Override public Row next() {

                                Row row = input.next();
                                if(row == null) {
                                    // todo
                                }
                                int idx = row.fieldIndex("sex");
                                Object obj = row.get(idx);
                                System.out.println(index + "++++++++++++" + row);
                                if(st == null) {
                                    StructType structt1 = row.schema();
                                    structt1 = structt1.add("tag", "int", true);
                                    st = structt1;
                                }
                                if(obj.equals(last)) {
                                    Object[] objs = new Object[st.length()];
                                    row.toSeq().copyToArray(objs);
                                    objs[st.length() -1 ] = index;
                                    Row rRow = new GenericRowWithSchema(objs, st);
                                    if(index > 2) {
                                        System.out.println("-------------" + row);
                                    }
                                    return rRow;
                                } else {
                                    last = obj;
                                    index ++;
                                    Object[] objs = new Object[st.length()];
                                    row.toSeq().copyToArray(objs);
                                    objs[st.length() -1 ] = index;
                                    Row rRow = new GenericRowWithSchema(objs, st);

//                                    Row rRow = new GenericRow(objs);
                                    if(index > 2) {
                                        System.out.println("-------------" + row);
                                    }
                                    return rRow;

                                }
                            }

                            @Override public void remove() {
                                input.remove();
                            }
                        };
                    }
                },
//                        org.apache.spark.sql.Encoders.kryo(Row.class)
                        en
                );

        tagDS.printSchema();

        List<Row> results = tagDS.collectAsList();
        for(Row r : results) {
            System.out.println(r);
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

}
