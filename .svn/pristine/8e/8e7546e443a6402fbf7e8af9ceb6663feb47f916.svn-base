package com.eurlanda.datashire.engine.entity.transformation;

import com.eurlanda.datashire.engine.entity.DataCell;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by zhudebin on 14-6-5.
 */
public abstract class TransformationProcessor implements Serializable {
    // 上游RDD
    protected JavaRDD<Map<Integer, DataCell>> inputRDD;


    abstract public JavaRDD<Map<Integer, DataCell>> process(JavaSparkContext jsc);

    public JavaRDD<Map<Integer, DataCell>> getInputRDD() {
        return inputRDD;
    }

    public void setInputRDD(JavaRDD<Map<Integer, DataCell>> inputRDD) {
        this.inputRDD = inputRDD;
    }

}
