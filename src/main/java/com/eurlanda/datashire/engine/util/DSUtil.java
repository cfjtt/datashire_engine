package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.spark.mllib.TrainDataConvertor;
import com.eurlanda.datashire.engine.spark.mllib.model.DiscretizeModel;
import com.eurlanda.datashire.engine.spark.mllib.model.QuantifyModel;
import com.eurlanda.datashire.engine.spark.mllib.normalize.NormalizerModel;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.PartialLeastSquaresRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.recommendation.ALSTrainModel;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhudebin on 15-4-18.
 */
public class DSUtil {

    private static Log log = LogFactory.getLog(DSUtil.class);

    public static boolean isNull(DataCell dc) {
        return dc == null || dc.getData() == null;
    }

    public static boolean isNotNull(DataCell dc) {
        return !isNull(dc);
    }

    public static DataCell predict(Object modelObj, SquidTypeEnum squidTypeEnum, DataCell inPara) {
        DataCell result;
        try {
            switch (squidTypeEnum) {
                case LOGREG:
                    org.apache.spark.ml.linalg.Vector predictDataLOGREG = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    LogisticRegressionModel lgModel = genModel(modelObj, LogisticRegressionModel.class);
                    if(predictDataLOGREG.size() != lgModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataLOGREG.size()+
                                "与训练数据的特征维数"+lgModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.INT, (int)lgModel.predict(predictDataLOGREG));
                    break;
                case LINEREG:
                    org.apache.spark.ml.linalg.Vector predictDataLINEREG = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    LinearRegressionModel lrModel = genModel(modelObj, LinearRegressionModel.class);
                    if(predictDataLINEREG.size() != lrModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataLINEREG.size() +
                                "与训练数据的特征维数"+lrModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.DOUBLE, lrModel.predict(predictDataLINEREG));
                    break;
                case RIDGEREG:
                    org.apache.spark.ml.linalg.Vector predictDataRIDGEREG = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    LinearRegressionModel lrRegModel = genModel(modelObj, LinearRegressionModel.class);
                    if(predictDataRIDGEREG.size() != lrRegModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataRIDGEREG.size() +
                                "与训练数据的特征维数"+lrRegModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.DOUBLE, lrRegModel.predict(predictDataRIDGEREG));
                    break;
                case SVM:
                    org.apache.spark.ml.linalg.Vector predictDataSVM= TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    SVMModel svmModel = genModel(modelObj, SVMModel.class);
                    if(predictDataSVM.size() != svmModel.weights().size()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataSVM.size() +
                                "与训练数据的特征维数"+svmModel.weights()+"不相等");
                    }
                    result = new DataCell(TDataType.INT, (int)svmModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case NAIVEBAYES:    // 朴素贝叶斯
                    org.apache.spark.ml.linalg.Vector predictDataNAIVEBAYES = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    NaiveBayesModel nbModel = genModel(modelObj, NaiveBayesModel.class);
                    if(predictDataNAIVEBAYES.size() != nbModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataNAIVEBAYES.size() +
                                "与训练数据的特征维数"+nbModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.INT, (int)nbModel.predict(TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString())));
                    break;
                case KMEANS:
                    KMeansModel kmModel = genModel(modelObj, KMeansModel.class);
                    if(kmModel.clusterCenters() == null || kmModel.clusterCenters().length ==0){
                        throw new RuntimeException("没有模型或模型是空");
                    }
                    org.apache.spark.ml.linalg.Vector prevcector = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    if(kmModel.clusterCenters()[0].size() != prevcector.size()){
                        throw new RuntimeException("预测数据的特征维数"+kmModel.clusterCenters()[0].size() +
                                "与训练数据的特征维数"+prevcector.size()+"不相等");
                    }
                    result = new DataCell(TDataType.INT, kmModel.predict(prevcector));
                    break;
                case BISECTINGKMEANSSQUID:
                    BisectingKMeansModel bisectingKMeansModel = genModel(modelObj, BisectingKMeansModel.class);
                    if(bisectingKMeansModel.clusterCenters() == null || bisectingKMeansModel.clusterCenters().length ==0){
                        throw new RuntimeException("没有模型或模型是空");
                    }
                    org.apache.spark.ml.linalg.Vector biKMeansPrevcector = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    int trainDmCount = bisectingKMeansModel.clusterCenters()[0].size();
                    int predictDmCount = biKMeansPrevcector.size();
                    if(trainDmCount != predictDmCount){
                        throw new RuntimeException("预测数据的特征维数"+predictDmCount+
                                "与训练数据的特征维数"+trainDmCount+"不相等");
                    }
                    result = new DataCell(TDataType.INT, bisectingKMeansModel.predict(biKMeansPrevcector));
                    break;
                case QUANTIFY:
                    QuantifyModel qModel = genModel(modelObj, QuantifyModel.class);
                    result = new DataCell(TDataType.DOUBLE, qModel.predict(inPara.getData()));
                    break;
                case DISCRETIZE:
                    DiscretizeModel dModel = genModel(modelObj, DiscretizeModel.class);
                    result = new DataCell(TDataType.DOUBLE, dModel.predict(ClassUtil.convert2Double(inPara.getData())));
                    break;
                case LASSO:
                    org.apache.spark.ml.regression.LinearRegressionModel linearRegressionModel = (org.apache.spark.ml.regression.LinearRegressionModel) modelObj;
                    result = new DataCell(TDataType.DOUBLE, linearRegressionModel.predict(TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString())));
                    break;
                case RANDOMFORESTCLASSIFIER:
                    RandomForestClassificationModel randomForestClassificationmodel = (RandomForestClassificationModel) modelObj;
                    result = new DataCell(TDataType.INT,
                            (int)randomForestClassificationmodel.predict(TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString())));
                    break;
                case RANDOMFORESTREGRESSION:
                    RandomForestRegressionModel randomForestRegressionModel = (RandomForestRegressionModel) modelObj;
                    result = new DataCell(TDataType.DOUBLE,
                            randomForestRegressionModel.predict(TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString())));
                    break;
                case MULTILAYERPERCEPERONCLASSIFIER:
                    MultilayerPerceptronClassificationModel mpcModel = (MultilayerPerceptronClassificationModel) modelObj;
                    result = new DataCell(TDataType.INT,
                            (int)mpcModel.predict(TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString())));
                    break;
                case NORMALIZER:
                    NormalizerModel normalizerModel = (NormalizerModel) modelObj;
                    result = new DataCell(TDataType.CSN, normalizerModel.predict(inPara.getData().toString()));
                    break;
                case PLS:
                    PartialLeastSquaresRegressionModel plsmodel = (PartialLeastSquaresRegressionModel) modelObj;
                    org.apache.spark.ml.linalg.DenseVector predictvector = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    if( predictvector.size() != plsmodel.numFeatures() ){
                        throw new RuntimeException("预测数据的特征维数"+predictvector.size() +
                                "与训练数据的特征维数"+plsmodel.numFeatures()+"不相等");
                    }
                    org.apache.spark.ml.linalg.DenseVector resVec = plsmodel.predictLabelVector(predictvector,false);
                    String resStr = resVec.apply(0)+"";
                    for(int i=1;i< resVec.values().length;i++){
                       resStr +=","+resVec.apply(i);
                    }
                    result = new DataCell(TDataType.CSN, resStr) ;
                    break;
                case DECISIONTREEREGRESSION:
                    org.apache.spark.ml.linalg.Vector predictDataDECISIONTREEREGRESSION = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    DecisionTreeRegressionModel decTreeRegModel = genModel(modelObj, DecisionTreeRegressionModel.class);
                    if(predictDataDECISIONTREEREGRESSION.size() != decTreeRegModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataDECISIONTREEREGRESSION.size() +
                                "与训练数据的特征维数"+decTreeRegModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.DOUBLE, decTreeRegModel.predict(predictDataDECISIONTREEREGRESSION));
                    break;
                case DECISIONTREECLASSIFICATION:
                    org.apache.spark.ml.linalg.Vector predictDataDECISIONTREECL = TrainDataConvertor.convertMLDoubleVector(inPara.getData().toString());
                    DecisionTreeClassificationModel decTreeClassModel = genModel(modelObj, DecisionTreeClassificationModel.class);
                    if(predictDataDECISIONTREECL.size() != decTreeClassModel.numFeatures()){
                        throw new RuntimeException("预测数据的特征维数"+predictDataDECISIONTREECL.size() +
                                "与训练数据的特征维数"+decTreeClassModel.numFeatures()+"不相等");
                    }
                    result = new DataCell(TDataType.INT, (int)decTreeClassModel.predict(predictDataDECISIONTREECL));
                    break;
                default:
                    throw new RuntimeException("DM 模型不匹配，" + squidTypeEnum);
            }
        return result;
        }catch (Throwable e){
            String errorMessage = e.getMessage();
            log.error(squidTypeEnum+"模型预测错误:" + errorMessage);
            if(errorMessage.contains("Dimension mismatch") || errorMessage.contains("non-matching sizes")||
              errorMessage.contains("Vectors must have same length")){
                throw new RuntimeException(squidTypeEnum+"：预测数据的特征维数与训练数据的特征维数不相等");
            }
           throw e;
        }
    }

    private static <T> T genModel(Object model, Class<T> c) {
        return (T)model;
    }

    public static JavaRDD<Map<Integer, DataCell>> predict(Object modelObj, SquidTypeEnum squidTypeEnum,
                                                          JavaRDD<java.util.Map<Integer, DataCell>> usersProducts,Integer inKey, Integer outKey) {
//        modelObj = ((Broadcast)modelObj).value();
        JavaRDD<Map<Integer, DataCell>> resultRDD ;
        try {
            switch (squidTypeEnum) {
                case ALS:
                    ALSTrainModel model = genModel(modelObj, ALSTrainModel.class);
                    // 初始化
                    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(usersProducts.rdd().context()) ;
                    model.init(jsc.sc());
                    // 预测
                   resultRDD = model.predict(usersProducts, inKey, outKey);
                    break;
                default:
                    throw new RuntimeException("DM 模型不匹配，" + squidTypeEnum);
            }
            return resultRDD;
        }catch (Throwable e){
            String errorMessage = e.getMessage();
            log.error(squidTypeEnum+"模型预测错误:" + errorMessage);
            if(errorMessage.contains("Dimension mismatch") || errorMessage.contains("non-matching sizes")||
                    errorMessage.contains("Vectors must have same length")){
                throw new RuntimeException(squidTypeEnum+"：预测数据的特征维数与训练数据的特征维数不相等");
            }
            throw e;
        }
    }

    /**
     * 是否需要缓存
     * @param squidName
     * @return
     */
    public static boolean isNeedCache(String squidName) {
        if(squidName.lastIndexOf("_c1_1") > 0
                ||  squidName.lastIndexOf("_c1_2") > 0
                ||  squidName.lastIndexOf("_c2_1") > 0
                ||  squidName.lastIndexOf("_c2_2") > 0
                ) {
            log.info("----------- 需要缓存 ------- " + squidName);
            return true;
        }
        return false;
    }

    /**
     * 缓存RDD
     * @param squidName
     * @param rdd
     * @return
     */
    public static JavaRDD<Map<Integer, DataCell>> cacheRDD(String squidName, JavaRDD<Map<Integer, DataCell>> rdd) {
        JavaRDD<Map<Integer, DataCell>> cachedRDD = null;
        if(squidName.lastIndexOf("_c1_1") > 0) {
            cachedRDD = rdd.persist(StorageLevel.MEMORY_ONLY());
        } else if(squidName.lastIndexOf("_c1_2") > 0) {
            cachedRDD = rdd.persist(StorageLevel.MEMORY_ONLY_SER());
        } else if(squidName.lastIndexOf("_c2_1") > 0) {
            cachedRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK());
        } else if(squidName.lastIndexOf("_c2_2") > 0) {
            cachedRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
        }

        return cachedRDD;
    }

    public static boolean isNeedCoalesce(String squidName) {
        Pattern pattern = Pattern.compile(".*_m\\d+$");
        Matcher matcher = pattern.matcher(squidName);
        return matcher.matches();
    }

    public static Dataset<Row> coalesceDataSet(String squidName, Dataset<Row> ds) {
        Pattern pattern = Pattern.compile(".*_m(\\d+)$");
        Matcher matcher = pattern.matcher(squidName);
        if(matcher.find()) {
            int num = Integer.parseInt(matcher.group(1));
            return ds.coalesce(num);
        }
        return ds;
    }

    /**
     * 缓存Dataset
     * @param squidName
     * @param ds
     * @return
     */
    public static Dataset<Row> cacheDataset(String squidName, Dataset<Row> ds) {
        Dataset<Row> cachedDS = null;
        if(squidName.lastIndexOf("_c1_1") > 0) {
            cachedDS = ds.persist(StorageLevel.MEMORY_ONLY());
        } else if(squidName.lastIndexOf("_c1_2") > 0) {
            cachedDS = ds.persist(StorageLevel.MEMORY_ONLY_SER());
        } else if(squidName.lastIndexOf("_c2_1") > 0) {
            cachedDS = ds.persist(StorageLevel.MEMORY_AND_DISK());
        } else if(squidName.lastIndexOf("_c2_2") > 0) {
            cachedDS = ds.persist(StorageLevel.MEMORY_AND_DISK_SER());
        }
        return cachedDS;
    }

    public static DataCell inverseNormalizer(Object modelObj, DataCell inPara) {
        NormalizerModel normalizerModel = (NormalizerModel) modelObj;
        DataCell result;
        if (inPara == null) {
            result = new DataCell(TDataType.STRING, "数据是null");
            return result;
        }
        String data = inPara.getData().toString();
        if (data == null || data.trim().equals("")) {
            result = new DataCell(TDataType.STRING, "数据是null或是空字符串");
            return result;
        } else {
            result = new DataCell(TDataType.CSN, normalizerModel.inverseNormalizer(data));
            return result;
        }
    }

    public static void main(String[] args) {
        String squidName = "aabc_m2001";
        System.out.println(isNeedCoalesce(squidName));
        System.out.println(coalesceDataSet(squidName, null));
    }
}
