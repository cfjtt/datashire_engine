package com.eurlanda.datashire.engine.entity.transformation;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALSTrainModel;

import java.util.Map;

/**
 * Created by zhudebin on 14-6-28.
 */
public class ALSPredictProcessor extends TransformationProcessor {

    private static Log log = LogFactory.getLog(ALSPredictProcessor.class);

    // 待做的transformation 操作
    private TTransformationAction tTransformationAction;

    public ALSPredictProcessor(JavaRDD<Map<Integer, DataCell>> inputRDD, TTransformationAction tTransformationAction) {
        this.inputRDD = inputRDD;
        this.tTransformationAction = tTransformationAction;
    }

    @Override
    public JavaRDD<Map<Integer, DataCell>> process(JavaSparkContext jsc) {
        // 获取模型
        Map<String, Object> infoMap = tTransformationAction.gettTransformation().getInfoMap();

        Integer inkey = tTransformationAction.gettTransformation().getInKeyList().get(0);
        Integer outkey = tTransformationAction.gettTransformation().getOutKeyList().get(0);

        // 模型类型
        ALSTrainModel model = (ALSTrainModel)(infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue));
//        ALSTrainModel model = (ALSTrainModel)(((Broadcast)infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue)).value());

        // 初始化
        model.init(jsc.sc());

        // 预测
        JavaRDD<Map<Integer, DataCell>> resultRDD = model.predict(inputRDD, inkey, outkey);

        return resultRDD;
    }
}
