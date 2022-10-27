package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-5-22.
 */
public class TMongoExtractSquidTest {
    CustomJavaSparkContext sc;

    @Before
    public void before() {
        sc = new CustomJavaSparkContext("local[4]", "test");
    }

    @Test
    public void testExtract() {

        List<TColumn> list = new ArrayList<>();
        list.add(new TColumn(TDataType.STRING, "_id", 1));
        list.add(new TColumn(TDataType.MAP, "k3.k2", 2));
        list.add(new TColumn(TDataType.STRING, "k1", 3));

        JavaRDD<Map<Integer, DataCell>> rdd = sc.mongoRDD("mongodb://192.168.137.4:27017/test.test2"
                ,"{\"k3.k1\":\"k1\"}",
                list.toArray(new TColumn[0]));

        long count = rdd.count();
        List<Map<Integer, DataCell>> result = rdd.collect();
        for(Map<Integer, DataCell> m : result) {
            System.out.println(m);
        }
        System.out.println(count);
    }

}
