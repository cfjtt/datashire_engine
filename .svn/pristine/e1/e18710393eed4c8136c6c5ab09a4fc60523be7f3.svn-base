package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.entity.transformation.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.spark.mllib.model.DiscretizeModel;
import com.eurlanda.datashire.engine.spark.mllib.model.QuantifyModel;
import com.eurlanda.datashire.engine.spark.mllib.normalize.NormalizerModel;
import com.eurlanda.datashire.engine.spark.util.EngineUtil;
import com.eurlanda.datashire.engine.translation.expression.ExpressionValidator;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.PartialLeastSquaresRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.recommendation.ALSTrainModel;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by zhudebin on 16/3/14.
 */
public class SquidUtil {

    private static Log log = LogFactory.getLog(SquidUtil.class);

    /**
     * 将RDD进行transformation处理
     *
     * @param preRDD                   输入RDD
     * @param tTransformationActions   待执行的transformation
     * @param isPreviousExceptionSquid 上游是否为ExceptionSquid
     * @param jobContext               TSquid.jobContext
     * @return 处理之后的 RDD
     */
    public static JavaRDD<Map<Integer, DataCell>> transformationRDD(
            JavaRDD<Map<Integer, DataCell>> preRDD,
            List<TTransformationAction> tTransformationActions,
            boolean isPreviousExceptionSquid,
            TJobContext jobContext) {
        List<TTransformationAction> tas = null;
        SparkContext sc = preRDD.context();
        JavaSparkContext jsc = new JavaSparkContext(sc);

        for (TTransformationAction ta : tTransformationActions) {
            TransformationTypeEnum type = ta.gettTransformation().getType();
            Map<String, Object> infoMap = ta.gettTransformation().getInfoMap();
            SquidTypeEnum squidTypeEnum = (SquidTypeEnum) infoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue);
            // 对预测的transformation,查询模型
            int inkeySize = ta.gettTransformation().getInKeyList().size();
            try {
                if (type == TransformationTypeEnum.PREDICT
                        || type == TransformationTypeEnum.INVERSEQUANTIFY
                        || type == TransformationTypeEnum.INVERSENORMALIZER) {
                    List<Integer> inKeys = ta.gettTransformation().getInKeyList();
                    if (inkeySize == 1) {
                        // 模型类型
                        String sql = (String) infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
                        //  Map<String,Object> data = ConstantUtil.getHbaseJdbc().queryForMap(sql);
                        Map<String, Object> data = ConstantUtil.getDataMiningJdbcTemplate().queryForMap(sql);
                        byte[] bytes = (byte[]) data.get("MODEL");
                        // 直接放 模型
                        Object model = genModel(bytes, squidTypeEnum);
                        // 广播一下提高性能
                        //  Object model = jsc.broadcast(model);

//                    model = EngineUtil.broadcast(model, sc);
                        infoMap.put(TTransformationInfoType.PREDICT_DM_MODEL.dbValue, model);
                    } else if (inkeySize == 2) {
                        // 存在key

                    }

                } else if (type == TransformationTypeEnum.RULESQUERY) {
                  //  String selectmaxversionsql = (String) infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
                    String selectmaxversionsql = (String) infoMap.get("AssociationRulesModelVersionSQL");
                    Object secVersionObj = ConstantUtil.getDataMiningJdbcTemplate().queryForList(selectmaxversionsql).get(0).get("maxModelVersion");
                    if(secVersionObj == null){ // 还没有模型
                        throw new RuntimeException("查询关联规则版本错误");
                    }else {
                        Integer secModelVersion = Integer.parseInt(secVersionObj.toString());
                        infoMap.put("AssociationRulesModelVersion", secModelVersion);
                    }
                }
            }catch (Exception e){
                String errorMessage = e.getMessage();
                log.error("模型预测异常:"+errorMessage);
                if(errorMessage.contains("Table") || errorMessage.contains("doesn't exist")){
                    throw new RuntimeException("模型表不存在，可能原因：没有训练模型");
                }else if(errorMessage.contains("Incorrect result size")){
                    throw new IllegalArgumentException("没有训练模型",e);
                }
                throw e;
            }
        //    if(type.isCommon(squidTypeEnum)) {
           if (isCommon(type,squidTypeEnum, inkeySize)) {
                if (tas == null) {
                    tas = new ArrayList<>();
                    tas.add(ta);
                } else {
                    tas.add(ta);
                }
            } else {
                if (tas != null) {
                    preRDD = new CommonProcessor(preRDD, tas, isPreviousExceptionSquid, jobContext).process(jsc);
                    tas = null;
                }
                // 处理非common的
                switch (type) {
                    case TOKENIZATION:
                        preRDD = new TokenizationProcessor(preRDD, ta).process(jsc);
                        break;
                    case AUTO_INCREMENT:
                        preRDD = new AutoIncrementProcessor(preRDD, ta).process(jsc);
                        break;
                    case PREDICT:
                    case INVERSENORMALIZER:
                        // als predict
                        if (squidTypeEnum == SquidTypeEnum.ALS && inkeySize == 1 ) {
                             preRDD = new ALSPredictProcessor(preRDD, ta).process(jsc);
                        }
                        // 存在key
                        else if (inkeySize == 2) {
                            preRDD = new PredictWithKeyProcessor(preRDD, ta).process(jsc);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("不存在该transformation" + type);
                }
            }
        }

        // 扫尾
        if (tas != null) {
            preRDD = new CommonProcessor(preRDD, tas, isPreviousExceptionSquid, jobContext).process(jsc);
        }

        return preRDD;
    }

    public static boolean isCommon(TransformationTypeEnum transformationTypeEnum,SquidTypeEnum squidTypeEnum, int inkeySize) {
        return transformationTypeEnum != TransformationTypeEnum.TOKENIZATION
                && transformationTypeEnum != TransformationTypeEnum.AUTO_INCREMENT
                && (transformationTypeEnum !=TransformationTypeEnum. PREDICT || squidTypeEnum != SquidTypeEnum.ALS)
                && (transformationTypeEnum != TransformationTypeEnum.PREDICT || inkeySize != 2)
                && (transformationTypeEnum != TransformationTypeEnum.INVERSENORMALIZER || inkeySize != 2);
    }

    public static JavaRDD<Map<Integer, DataCell>> unionRDD(
            List<JavaRDD<Map<Integer, DataCell>>> preRDDs,
            final List<Integer> leftInKeyList,
            final List<Integer> rightInKeyList, TUnionType unionType) {
        JavaRDD<Map<Integer, DataCell>> leftRDD = preRDDs.get(0);
        JavaRDD<Map<Integer, DataCell>> rightRDD = preRDDs.get(1);
        rightRDD = rightRDD.map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Map<Integer, DataCell> map) throws Exception {
                Map<Integer, DataCell> result = new HashMap<>();
                int i = 0;
                for (Integer key : rightInKeyList) {
                    DataCell cell = map.get(key);
                    if (cell != null) {
                        result.put(leftInKeyList.get(i), cell);
                    }
                    i++;
                }
                return result;
            }
        });
        JavaRDD<Map<Integer, DataCell>> resultRDD = leftRDD.union(rightRDD);
        switch (unionType) {
            case UNION_ALL:
                break;
            case UNION:
                resultRDD = resultRDD.distinct();
                break;
        }
        return resultRDD;
    }

    public static JavaRDD<Map<Integer, DataCell>> filterRDD(JavaRDD<Map<Integer, DataCell>> preRDD,
                                                            final TFilterExpression filterExpression) {
        final Broadcast fe = EngineUtil.broadcast(filterExpression, preRDD.context());
        return preRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
            @Override
            public Boolean call(Map<Integer, DataCell> map) throws Exception {
                TFilterExpression tfe = (TFilterExpression) fe.getValue();
                if (tfe == null) return true;
                return ExpressionValidator.validate(tfe, map);
            }
        });

    }

    public static JavaRDD<Map<Integer, DataCell>> aggregateRDD(JavaRDD<Map<Integer, DataCell>> preRDD,
                                                               final List<Integer> groupKeyList,
                                                               final List<AggregateAction> aaList) {
        // 分组
        JavaPairRDD<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>>
                jpr = preRDD.groupBy(new Function<Map<Integer, DataCell>, ArrayList<DataCell>>() {
            @Override
            public ArrayList<DataCell> call(Map<Integer, DataCell> map) throws Exception {
                ArrayList<DataCell> keyGroup = new ArrayList<>();
                // 判断是否有group by
                if (groupKeyList != null && groupKeyList.size() > 0) {
                    for (Integer i : groupKeyList) {
                        if (DSUtil.isNotNull(map.get(i))) {
                            keyGroup.add(map.get(i));
                        } else {
                            keyGroup.add(null);
                        }
                    }
                } else {
                    // 如果没有分组，那么所有数据分成一组
                    // TODO 默认分组采用的reduce 分组合并，可以考虑使用map端reduce再分组
                }

                return keyGroup;
            }
        });

        // 在翻译时 对aaList中一个column中既存在goup,又存在 aggregate的，只保留aggregate
        List<TOrderItem> orders = null;
        for (AggregateAction aa : aaList) {
            if (aa.getOrders() != null) {
                if (orders == null) {
                    orders = aa.getOrders();
                } else {
                    throw new RuntimeException("一个stageSquid中只能有一个 (first_value|last_value)聚合");
                }
            }
        }
        final List<TOrderItem> finalOrders = orders;
        return jpr.map(new Function<Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Tuple2<ArrayList<DataCell>, Iterable<Map<Integer, DataCell>>> t2) throws Exception {
                // 聚合操作实际运行
                return aggregate(t2._2().iterator(), aaList, finalOrders);
            }
        });
    }

    /**
     * @param iter
     * @param aaList
     * @return
     */
    private static Map<Integer, DataCell> aggregate(Iterator<Map<Integer, DataCell>> iter, List<AggregateAction> aaList, List<TOrderItem> orders) {
        Map<Integer, Object> aggreMap = new HashMap<>();

        long count = 0l;
        while (iter.hasNext()) {
            count++;
            Map<Integer, DataCell> row = iter.next();
            for (AggregateAction aa : aaList) {

                Object aggInterData = aggreMap.get(aa.getOutKey());
                int outKey = aa.getOutKey();
                int inKey = aa.getInKey();
                // 聚合列的值
                DataCell dc = row.get(inKey);
                boolean isNotNull = DSUtil.isNotNull(dc);
                // 根据聚合来取值
                switch (aa.getType()) {
                    case SORT:
                        break;
                    case GROUP:
                        if (aggInterData == null) {
                            if (isNotNull) {
                                aggreMap.put(outKey, dc.clone());
                            }
                        }
                        break;
                    case AVG:
                        // 判断是否为空
                        if (isNotNull) {
                            if (aggInterData == null) {
                                aggreMap.put(outKey, new AvgItem(dc.clone()));
                            } else {
                                ((AvgItem) aggInterData).add(dc);
                            }
                        }
                        break;
                    case COUNT:
                        break;
                    case FIRST_VALUE:
                        if (aggInterData == null) {
                            aggreMap.put(outKey, row);
                        } else {
                            // 根据排序字段比较当前值
                            int compareResult = OrderUtil.compare((Map<Integer, DataCell>) aggInterData, row, orders);
                            if (compareResult < 0) {
                                aggreMap.put(outKey, row);
                            }
                        }
                        break;
                    case LAST_VALUE:
                        if (aggInterData == null) {
                            aggreMap.put(outKey, row);
                        } else {
                            // 根据排序字段比较当前值
                            int compareResult = OrderUtil.compare((Map<Integer, DataCell>) aggInterData, row, orders);
                            if (compareResult > 0) {
                                aggreMap.put(outKey, row);
                            }
                        }
                        break;
                    case MAX:
                        if (isNotNull) {
                            if (aggInterData == null) {
                                aggreMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell) aggInterData).max(dc);
                            }
                        }
                        break;
                    case MIN:
                        if (isNotNull) {
                            if (aggInterData == null) {
                                aggreMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell) aggInterData).min(dc);
                            }
                        }
                        break;
                    case SUM:
                        if (isNotNull) {
                            if (aggInterData == null) {
                                aggreMap.put(outKey, dc.clone());
                            } else {
                                ((DataCell) aggInterData).add(dc);
                            }
                        }
                        break;
                    case STRING_SUM:
                        if (isNotNull) {
                            if (aggInterData == null) {
                                 aggreMap.put(outKey, new DataCell(TDataType.CSV,dc.getData()));
                            } else {
                                ((DataCell) aggInterData).add(new DataCell(TDataType.CSV,dc.getData()));
                            }
                        }
                        break;
                    default:
                        throw new RuntimeException("没有该类型的聚合:" + aa.getType().name());
                }
            }
        }

        Map<Integer, DataCell> outMap = new HashMap<>();
        int last_first_key = 0;
        for (AggregateAction aa : aaList) {
            // 获取first,last 的输出列
            switch (aa.getType()) {
                case FIRST_VALUE:
                case LAST_VALUE:
                    last_first_key = aa.getOutKey();
                    break;
            }
        }
        for (AggregateAction aa : aaList) {
            int outKey = aa.getOutKey();
            switch (aa.getType()) {
                case AVG:
                    AvgItem ai = (AvgItem) aggreMap.get(outKey);
                    if (ai == null) {
                        outMap.put(outKey, null);
                    } else {
                        outMap.put(outKey, ai.avg());
                    }
                    break;
                case COUNT:
                    outMap.put(outKey, new DataCell(TDataType.LONG, count));
                    break;
                case FIRST_VALUE:
                case LAST_VALUE:
                    outMap.put(outKey, ((Map<Integer, DataCell>) aggreMap.get(outKey)).get(aa.getInKey()));
                    break;
                case SORT:
                    if (last_first_key != 0) {
                        outMap.put(outKey, ((Map<Integer, DataCell>) aggreMap.get(last_first_key)).get(aa.getInKey()));
                    }
                    break;
                default:
                    outMap.put(aa.getOutKey(), (DataCell) aggreMap.get(aa.getOutKey()));
            }

        }

        //取小数位数
        for (AggregateAction aa : aaList) {
            switch (aa.getType()) {
                case SORT:
                case AVG:
                case SUM:
                case MAX:
                case MIN:
                case FIRST_VALUE:
                case LAST_VALUE:
                    for (Integer k : outMap.keySet()) {

                        // todo 如果数值类型放开,就需要对类型在此处进行转换,并校验
                        if(DSUtil.isNotNull(outMap.get(k))) {
                            // 只对 decimal类型检验精度
                            if(outMap.get(k).getdType() == TDataType.BIG_DECIMAL) {
                                int scala = aa.getColumn().getScale();//要保留的小数位数
                                BigDecimal bigDecimal = (BigDecimal)outMap.get(k).getData();
                                DataCell result = new DataCell(outMap.get(k).getdType(),
                                        bigDecimal.setScale(scala, BigDecimal.ROUND_HALF_UP));
                                outMap.put(k, result);
                            } else {
                                log.info("不需要检验精度的值");
                            }
                        }
                    }
                    break;
            }
        }

        return outMap;
    }

    public static Object genModelFromResult(Map<String, Object> result, SquidTypeEnum squidTypeEnum) {
        return genModel((byte[]) result.get("model"), squidTypeEnum);
    }

    public static Object genModel(byte[] bytes, SquidTypeEnum squidTypeEnum) {
        Object model = null;
        switch (squidTypeEnum) {
            case LOGREG:
                model = genModel(bytes, LogisticRegressionModel.class);
                break;
            case LINEREG:
                model = genModel(bytes, LinearRegressionModel.class);
                break;
            case RIDGEREG:
                model = genModel(bytes, LinearRegressionModel.class);
                break;
            case SVM:
                model = genModel(bytes, SVMModel.class);
                break;
            case NAIVEBAYES:    // 朴素贝叶斯
                model = genModel(bytes, NaiveBayesModel.class);
                break;
            case ALS:
                model = genModel(bytes, ALSTrainModel.class);
                break;
            case KMEANS:
                model = genModel(bytes, KMeansModel.class);
                break;
            case QUANTIFY:
                model = genModel(bytes, QuantifyModel.class);
                break;
            case DISCRETIZE:
                model = genModel(bytes, DiscretizeModel.class);
                break;
          /*  case DECISIONTREE:
                model = genModel(bytes, DecisionTreeModel.class);
                break;*/
            case LASSO:
              //  String csn = new String(bytes);
              //  model = genLinearRegressionModel(csn);
                model = genModel(bytes, LinearRegressionModel.class);
                break;
            case RANDOMFORESTCLASSIFIER:
                model = genModel(bytes, RandomForestClassificationModel.class);
                break;
            case RANDOMFORESTREGRESSION:
                model = genModel(bytes, RandomForestRegressionModel.class);
                break;
            case MULTILAYERPERCEPERONCLASSIFIER:
                model = genModel(bytes, MultilayerPerceptronClassificationModel.class);
                break;
            case NORMALIZER:
                model = genModel(bytes, NormalizerModel.class);
                break;
            case PLS:
                model = genModel(bytes, PartialLeastSquaresRegressionModel.class);
                break;
            case DECISIONTREEREGRESSION:
                model = genModel(bytes, DecisionTreeRegressionModel.class);
                break;
            case DECISIONTREECLASSIFICATION:
                model = genModel(bytes, DecisionTreeClassificationModel.class);
                break;
            case BISECTINGKMEANSSQUID:
                model = genModel(bytes, BisectingKMeansModel.class);
                break;
            default:
                throw new RuntimeException("DM 模型不匹配，" + squidTypeEnum);
        }
        return model;
    }

    private static <T> T genModel(byte[] bytes, Class<T> c) {
        try {
            return IOUtils.genObjectFromBytes(bytes, c);
        } catch (Exception e) {
            log.error("训练模型 bytes to Object 异常", e);
            throw new RuntimeException("训练模型 bytes to Object 异常", e);
        }
    }

    /**
     *
 //    * @param  第一个元素是 intercept, 后面元素是 weights
     * @return
     */
 /*   public static LinearRegressionModel genLinearRegressionModel(String csnModel){
        String[] modelString = csnModel.split(",");
        double intercept = Double.parseDouble(modelString[0]); // 第一个元素是 intercept
        double[] weightsArr = new double[modelString.length-1];// 后面元素是 weights
        for(int i = 1;i<modelString.length;i++ ){
            weightsArr[i-1] = Double.parseDouble(modelString[i]);
        }
        org.apache.spark.mllib.linalg.Vector weights = org.apache.spark.mllib.linalg.Vectors.dense(weightsArr);
        LinearRegressionModel linearRegressionModel = new LinearRegressionModel(weights,intercept);
        return linearRegressionModel;
    }*/

    public static class CountItem implements Serializable {
        private long total = 0;

        public CountItem(long total) {
            this.total = total;
        }

        public void addOne() {
            total ++;
        }

        public void add(CountItem countItem) {
            this.total += countItem.total;
        }

        public long count() {
            return total;
        }
    }

    public static class AvgItem implements Serializable {
        private DataCell dc;
        private long total;

        public AvgItem(DataCell dc) {
            this.dc = dc;
            this.total = 1;
        }

        public void add(DataCell dc) {
            this.dc.add(dc);
            this.total++;
        }

        public void add(AvgItem avgItem) {
            this.dc.add(avgItem.dc);
            this.total += avgItem.total;
        }

        public DataCell avg() {
            return avg(dc, total);
        }

        public DataCell avg(DataCell count, long nums) {
            DataCell dc = new DataCell();
            switch (count.getdType()) {
                case LONG:
                    dc.setData(((Long) count.getData() + 0.0d) / nums);
                    dc.setdType(TDataType.DOUBLE);
                    break;
                case FLOAT:
                    dc.setData((float) count.getData() / nums);
                    dc.setdType(TDataType.FLOAT);
                    break;
                case DOUBLE:
                    dc.setData((double) count.getData() / nums);
                    dc.setdType(TDataType.DOUBLE);
                    break;
                case TINYINT:
                    dc.setData(((byte) count.getData() + 0.0d) / nums);
                    dc.setdType(TDataType.DOUBLE);
                    break;
                case SHORT:
                    dc.setData(((short) count.getData() + 0.0d) / nums);
                    dc.setdType(TDataType.DOUBLE);
                    break;
                case INT:
                    dc.setData(((int) count.getData() + 0.0d) / nums);
                    dc.setdType(TDataType.DOUBLE);
                    break;
                case BIG_DECIMAL:
                    dc.setData(((BigDecimal) count.getData()).divide(BigDecimal.valueOf(nums), 5));
                    dc.setdType(TDataType.BIG_DECIMAL);
                    break;
                default:
                    throw new RuntimeException("无法对该数据类型[" + count.getdType() + "] 求平均值");
            }
            return dc;
        }
    }
}
