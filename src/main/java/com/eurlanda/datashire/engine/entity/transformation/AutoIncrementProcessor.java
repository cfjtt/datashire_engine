package com.eurlanda.datashire.engine.entity.transformation;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.spark.util.ScalaMethodUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * Created by zhudebin on 14-6-23.
 */
public class AutoIncrementProcessor extends TransformationProcessor {

    // 每个分片最大数
    private static Long maxPartition = 1000000000l;

    private static Log log = LogFactory.getLog(CommonProcessor.class);

    // 待做的transformation 操作
    private TTransformationAction tTransformationAction;

    public AutoIncrementProcessor(JavaRDD<Map<Integer, DataCell>> inputRDD, TTransformationAction tTransformationAction) {
        this.inputRDD = inputRDD;
        this.tTransformationAction = tTransformationAction;
    }

    @Override
    public JavaRDD<Map<Integer, DataCell>> process(JavaSparkContext jsc) {
        Partition[] ps = inputRDD.rdd().partitions();

        // 获取最小值
        long min = Long.parseLong(tTransformationAction.gettTransformation().getInfoMap().get(TTransformationInfoType.ID_AUTO_INCREMENT_MIN.dbValue).toString());
        int outKey = tTransformationAction.gettTransformation().getOutKeyList().get(0);

        // 采用自增  todo 暂时方案
        return ScalaMethodUtil.mapPartitionsWithIndex(inputRDD, min, outKey);

//        inputRDD.mapPartitionsWithIndex()

        // 获取最小值
//        inputRDD.rdd().zipPartitions()
//        inputRDD.rdd().mapPartitionsWithIndex()
    }
}
