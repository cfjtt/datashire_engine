package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import com.eurlanda.datashire.engine.util.ServerRpcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class TSquid implements ESquid,Serializable {

    private static Log log = LogFactory.getLog(TSquid.class);

    // tSquid 主键，用于唯一区别
    private String id;
    private String name;
    // 当前tSquid是由哪个Squid翻译过来得
    private int squidId;
    private TSquidType type;
    // 待处理的数据集
    transient protected JavaRDD<Map<Integer, DataCell>> outRDD;
    // dataFrame 数据集
    transient protected Dataset<Row> outDataFrame;
    protected String taskId;
    //判断squid是否完成
    private boolean isFinished = false;
    transient private TSquidFlow currentFlow;
    transient private TJobContext jobContext;

    // 依赖的squid
    protected List<TSquid> dependenceSquids;

    protected void executeDependencies(JavaSparkContext jsc) {
        if(dependenceSquids == null || dependenceSquids.size() == 0) {
            return;
        }
        for(TSquid tSquid : dependenceSquids) {
            if(!tSquid.isFinished()) {
                try {
                    tSquid.runSquid(jsc);
                    tSquid.setFinished();
                } catch (EngineException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * squid运行入口
     */
    public Object runSquid(JavaSparkContext jsc) throws EngineException {
        // 判断squidflow 可运行状态
        if(currentFlow.isDebugModel() && (!currentFlow.isSfFlag())) {
            log.info("squidflow 不可运行，跳过该squid执行....");
            return null;
        }

        Object result;
        try {
            // 判断依赖的是否已经运行 dependencies
            executeDependencies(jsc);
            // 发送squid状态：启动
            ServerRpcUtil.sendSquidStartStatus(this, getTaskId(), currentFlow);

            logSquidStart();
//            EngineLogFactory.logInfo(this, "引擎squid任务开始运行...");
            result = this.run(jsc);
            this.setFinished();
//            EngineLogFactory.logInfo(this, "引擎squid任务运行成功.");
            logSquidEnd();

            // 判断是否为调试模式, 发送squid状态：成功
            ServerRpcUtil.sendSquidSuccessStatus(this, getTaskId(), currentFlow);
            clean();
        } catch (Exception e) {
            // 判断是否为调试模式, 发送squid状态：失败
            ServerRpcUtil.sendSquidFailStatus(this, getTaskId(), currentFlow);
            EngineLogFactory.logError(this, "引擎squid任务运行失败.", e);
            // 设置运行失败
            currentFlow.setSfFlag(false);
            throw new RuntimeException(e);
        }
        return result;
    }

    private void logSquidStart() {
        String str1 = " 开始执行[";
        String str2 = "]";
        switch (this.getType()) {

            case TRANSFORMATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "transformation阶段" + str2);
                break;
            case AGGREGATE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "aggregate阶段" + str2);
                break;
            case DATA_FALL_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "数据落地阶段" + str2);
                break;
            case DEST_HDFS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "hdfs落地阶段" + str2);
                break;
            case DEST_IMPALA_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "impala落地阶段" + str2);
                break;
            case DISTINCT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "distinct阶段" + str2);
                break;
            case EXCEPTION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "exception处理阶段" + str2);
                break;
            case FILTER_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "filter阶段" + str2);
                break;
            /**
             * extractSquid
             */
            case DATABASE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "数据库抽取阶段" + str2);
                break;
            case HDFS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "hdfs抽取阶段" + str2);
                break;
            case FTP_FILE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "ftp文件抽取阶段" + str2);
                break;
            case SHARED_FILE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "共享文件夹文件抽取阶段" + str2);
                break;
            // 内部抽取
            case INNEREXTRACT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "内部抽取阶段" + str2);
                break;
            case REPORT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "report阶段" + str2);
                break;
            case JOIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "join阶段" + str2);
                break;
            case UNION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "union阶段" + str2);
                break;
            // 数据挖掘squid
            case ALS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "als train 阶段" + str2);
                break;
            case DECISIONTREE_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "decisiontree train 阶段" + str2);
                break;
            case DISCRETIZE_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "discretize train 阶段" + str2);
                break;
            case KMEANS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "kmeans train 阶段" + str2);
                break;
            case LINEARREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "linearregression train 阶段" + str2);
                break;
            case LOGISTICREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "logisticregression train 阶段" + str2);
                break;
            case NAIVEBAYS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "naivebays train 阶段" + str2);
                break;
            case QUANTIFY_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "quantify train阶段" + str2);
                break;
            case RIDGEREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "ridgeregression train 阶段" + str2);
                break;
            case ASSOCIATIONRULES_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "assocoationrules train 阶段" + str2);
                break;
            case RANDOMFOREST_CLASSIFICATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "randomforest_classification train 阶段" + str2);
                break;
            case LASSOREGRESSION_WITH_ELASTICNET_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "lassoregression_with_elasticnet train 阶段" + str2);
                break;
            case RANDOMFOREST_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "randomforest_regression train 阶段" + str2);
                break;
            case MULTILAYER_PERCEPTRON_CLASSIFICATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "multilayer_perceptron_classification train 阶段" + str2);
                break;
            case NORMALIZER_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "Normalizer train 阶段" + str2);
                break;
            case PARTIALLEASTSQUARES_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "Partial_Least_Squares_regression train 阶段" + str2);
                break;
            case DECISION_TREE_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "DECISION_TREE_REGRESSION_SQUID train 阶段" + str2);
                break;
            case DECISION_TREE_CLASSIFICATIONS_QUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "DECISION_TREE_CLASSIFICATION_SQUID train 阶段" + str2);
                break;
            case LOGISTIC_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "LOGISTIC_REGRESSION_SQUID train 阶段" + str2);
                break;
            case LINEAR_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "LINEAR_REGRESSION_SQUID train 阶段" + str2);
                break;
            case RIDGE_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "RIDGE_REGRESSION_SQUID train 阶段" + str2);
                break;
            case BISECTING_K_MEANS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "BISECTING_K_MEANS_SQUID train 阶段" + str2);
                break;
            default:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + this.getType().name() + str2);

        }
    }

    private void logSquidEnd() {
        String str1 = "[";
        String str2 = "] 执行完毕";
        switch (this.getType()) {

            case TRANSFORMATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "transformation阶段" + str2);
                break;
            case AGGREGATE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "aggregate阶段" + str2);
                break;
            case DATA_FALL_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "数据落地阶段" + str2);
                break;
            case DEST_HDFS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "hdfs落地阶段" + str2);
                break;
            case DEST_IMPALA_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "impala落地阶段" + str2);
                break;
            case DISTINCT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "distinct阶段" + str2);
                break;
            case EXCEPTION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "exception处理阶段" + str2);
                break;
            case FILTER_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "filter阶段" + str2);
                break;
            /**
             * extractSquid
             */
            case DATABASE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "数据库抽取阶段" + str2);
                break;
            case HDFS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "hdfs抽取阶段" + str2);
                break;
            case FTP_FILE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "ftp文件抽取阶段" + str2);
                break;
            case SHARED_FILE_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "共享文件夹文件抽取阶段" + str2);
                break;
            // 内部抽取
            case INNEREXTRACT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "内部抽取阶段" + str2);
                break;
            case REPORT_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "report阶段" + str2);
                break;
            case JOIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "join阶段" + str2);
                break;
            case UNION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "union阶段" + str2);
                break;
            // 数据挖掘squid
            case ALS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "als train 阶段" + str2);
                break;
            case DECISIONTREE_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "decisiontree train 阶段" + str2);
                break;
            case DISCRETIZE_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "discretize train 阶段" + str2);
                break;
            case KMEANS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "kmeans train 阶段" + str2);
                break;
            case LINEARREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "linearregression train 阶段" + str2);
                break;
            case LOGISTICREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "logisticregression train 阶段" + str2);
                break;
            case NAIVEBAYS_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "naivebays train 阶段" + str2);
                break;
            case QUANTIFY_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "quantify train阶段" + str2);
                break;
            case RIDGEREGRESSION_TRAIN_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "ridgeregression train 阶段" + str2);
                break;
            case ASSOCIATIONRULES_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "assocoationrules train 阶段" + str2);
                break;
            case RANDOMFOREST_CLASSIFICATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "randomforest_classification train 阶段" + str2);
                break;
            case LASSOREGRESSION_WITH_ELASTICNET_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "lassoregression_with_elasticnet train 阶段" + str2);
                break;
            case RANDOMFOREST_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "randomforest_regression train 阶段" + str2);
                break;
            case MULTILAYER_PERCEPTRON_CLASSIFICATION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "multilayer_perceptron_classification train 阶段" + str2);
                break;
            case NORMALIZER_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "NORMALIZER_SQUID train 阶段" + str2);
                break;
            case PARTIALLEASTSQUARES_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "PARTIALLEASTSQUARES_SQUID train 阶段" + str2);
                break;
            case DECISION_TREE_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "DECISION_TREE_REGRESSION_SQUID train 阶段" + str2);
                break;
            case DECISION_TREE_CLASSIFICATIONS_QUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "DECISION_TREE_CLASSIFICATION_SQUID train 阶段" + str2);
                break;
            case LOGISTIC_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "LOGISTIC_REGRESSION_SQUID train 阶段" + str2);
                break;
            case LINEAR_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "LINEAR_REGRESSION_SQUID train 阶段" + str2);
                break;
            case RIDGE_REGRESSION_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "RIDGE_REGRESSION_SQUID train 阶段" + str2);
                break;
            case BISECTING_K_MEANS_SQUID:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + "BISECTING_K_MEANS_SQUID train 阶段" + str2);
                break;
            default:
                EngineLogFactory.logInfo(this, this.getName()+ str1 + this.getType().name() + str2);
        }
    }

    /**
     * 清理工作
     */
    protected void clean() {
        log.debug("clean .....");
    }

    public boolean isDebug() {
        return currentFlow.isDebugModel();
    }

    protected abstract Object run(JavaSparkContext jsc) throws EngineException;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TSquidType getType() {
        return type;
    }

    public void setType(TSquidType type) {
        this.type = type;
    }

    public JavaRDD<Map<Integer, DataCell>> getOutRDD() {
        return outRDD;
    }

    public void setOutRDD(JavaRDD<Map<Integer, DataCell>> outRDD) {
        this.outRDD = outRDD;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public int getSquidId() {
        return squidId;
    }

    public void setSquidId(int squidId) {
        this.squidId = squidId;
    }

    private void setFinished() {
        isFinished = true;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public TSquidFlow getCurrentFlow() {
        return currentFlow;
    }

    public void setCurrentFlow(TSquidFlow currentFlow) {
        if (currentFlow != null) {
            setTaskId(currentFlow.getTaskId());
            this.setJobContext(currentFlow.getJobContext());
            this.jobContext.from(this);
        }
        this.currentFlow = currentFlow;
    }

    public TJobContext getJobContext() {
        return jobContext;
    }

    public void setJobContext(TJobContext jobContext) {
        this.jobContext = jobContext;
    }

    public String getName() {
        return name;
    }

    public List<TSquid> getDependenceSquids() {
        return dependenceSquids;
    }

    public void setDependenceSquids(List<TSquid> dependenceSquids) {
        this.dependenceSquids = dependenceSquids;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Dataset<Row> getOutDataFrame() {
        return outDataFrame;
    }

    public void setOutDataFrame(Dataset<Row> outDataFrame) {
        this.outDataFrame = outDataFrame;
    }
}
