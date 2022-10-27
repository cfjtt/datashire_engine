package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.mllib.ALSTrainSquid;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.DataCellUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.CustomJavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALSTrainModel;
import scala.Tuple3;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * ALS squid
 * Created by zhudebin on 14-5-26.
 */
public class TALSTrainSquid extends TTrainSquid {

    private static Log log = LogFactory.getLog(TALSTrainSquid.class);

    // Regularization，double，正则化
    private double lambda = 0.01;
    private int rank = 1;
    // ImplicitPreferences
    private boolean implicitPrefs = false;
    private double alpha = 1.0;
    // 最大迭代次数
    private int iterationNumber = 10;
    // ParallelRuns（并行数）
    private int numBlocks = 10;
    private long seed = 1;

    public TALSTrainSquid() {
        this.setType(TSquidType.ALS_TRAIN_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        if (preSquid.getOutRDD() == null) {
            preSquid.runSquid(jsc);
        }
        // 判断是否保存历史，如果保存历史查询最大版本号，否则清空历史数据

        java.sql.Connection conn = null;
        try {
            String tableName = getTableName();
            String saveModelSql = getSaveModelSql(tableName);
            conn = getConnectionFromDS();
            int modelVersion = init(conn,tableName);
            if (key > 0) {
                JavaRDD<Map<Integer, DataCell>> preRDD = preSquid.getOutRDD().cache();
                List<DataCell> list = preRDD.map(new Function<Map<Integer, DataCell>, DataCell>() {
                    @Override
                    public DataCell call(Map<Integer, DataCell> v1) throws Exception {
                        return v1.get(key);
                    }
                }).distinct().collect();

                for (final DataCell dc : list) {
                    JavaRDD<Map<Integer, DataCell>> filterRDD = preRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
                        @Override
                        public Boolean call(Map<Integer, DataCell> v1) throws Exception {
                            if (DSUtil.isNull(dc)) {
                                return DSUtil.isNull(v1.get(key));
                            } else {
                                return dc.equals(v1.get(key));
                            }
                        }
                    });
                    // 模型，模型精确性，总数
                    Tuple3<ALSTrainModel, Float, Long> t3 =
                            new ALSTrainSquid(filterRDD, inKey, percentage,
                                    rank, lambda, implicitPrefs, alpha, iterationNumber, numBlocks, seed)
                                    .run((CustomJavaSparkContext) jsc);
                  //  saveRecord(t3._1(), t3._2(), t3._3(), DataCellUtil.getData(dc), tableName, version);
                    saveModel(conn,tableName,saveModelSql, DataCellUtil.getData(dc).toString(),
                              modelVersion,t3._1(), t3._2(), t3._3());
                }
            } else {
                Tuple3<ALSTrainModel, Float, Long> t3 =
                        new ALSTrainSquid(preSquid.getOutRDD(), inKey, percentage,
                                rank, lambda, implicitPrefs, alpha, iterationNumber, numBlocks, seed)
                                .run((CustomJavaSparkContext) jsc);
            //     saveRecord(t3._1(),t3._2(), t3._3());
                saveModel(conn,tableName,saveModelSql, key+"", modelVersion,t3._1(), t3._2(), t3._3());
                return t3._1();
            }
            return null;
        } catch (Throwable e) {
            log.error("训练或保存模型异常", e);
            throw new RuntimeException(e);
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public double getLambda() {
        return lambda;
    }

    public TALSTrainSquid setLambda(double lambda) {
        this.lambda = lambda;
        return this;
    }

    public int getRank() {
        return rank;
    }

    public TALSTrainSquid setRank(int rank) {
        this.rank = rank;
        return this;
    }

    public boolean isImplicitPrefs() {
        return implicitPrefs;
    }

    public TALSTrainSquid setImplicitPrefs(boolean implicitPrefs) {
        this.implicitPrefs = implicitPrefs;
        return this;
    }

    public double getAlpha() {
        return alpha;
    }

    public TALSTrainSquid setAlpha(double alpha) {
        this.alpha = alpha;
        return this;
    }

    public int getIterationNumber() {
        return iterationNumber;
    }

    public TALSTrainSquid setIterationNumber(int iterationNumber) {
        this.iterationNumber = iterationNumber;
        return this;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public TALSTrainSquid setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
        return this;
    }

    public long getSeed() {
        return seed;
    }

    public TALSTrainSquid setSeed(long seed) {
        this.seed = seed;
        return this;
    }
}
