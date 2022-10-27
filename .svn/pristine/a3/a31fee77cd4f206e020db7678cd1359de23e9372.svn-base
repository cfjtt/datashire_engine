package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.mllib.DiscretizeSquid;
import com.eurlanda.datashire.engine.spark.mllib.model.DiscretizeModel;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.DataCellUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 14-6-9.
 * 离散化 squid
 * 训练一个离散化的squid
 */
public class TDiscretizeTrainSquid extends TTrainSquid {

    private static Log log = LogFactory.getLog(TDiscretizeTrainSquid.class);

    // 离散区间数
    private int buckets;

    public TDiscretizeTrainSquid() {
        this.setType(TSquidType.DISCRETIZE_TRAIN_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        if (preSquid.getOutRDD() == null) {
            preSquid.runSquid(jsc);
        }
        percentage = 1.0F; // 离散化的训练比例100%，没有测试数据
        // 判断是否保存历史，如果保存历史查询最大版本号，否则清空历史数据
        java.sql.Connection conn = null;
        try {
            String tableName = getTableName();
            conn = getConnectionFromDS();
            String saveModelSql = getSaveModelSql(tableName);
            int version = init(conn,tableName);
            if (key > 0) {  // 根据key来分组建模
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
                    Tuple3<DiscretizeModel, Float, Long> t3 = new DiscretizeSquid(filterRDD, inKey, buckets).run();
                    saveModel(conn, tableName, saveModelSql, DataCellUtil.getData(dc).toString(), version, t3._1(), t3._2(), t3._3());
                }
            } else {
                // 模型，模型精确性，总数
                DiscretizeSquid discretizeSquid = new DiscretizeSquid(preSquid.getOutRDD(), inKey, buckets);
                Tuple3<DiscretizeModel, Float, Long> t3 = discretizeSquid.run();
                saveModel(conn, tableName, saveModelSql, key + "", version, t3._1(), t3._2(), t3._3());
            }
            return null;
        }catch (Throwable e){
            log.error("训练或保存模型异常", e);
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int getBuckets() {
        return buckets;
    }

    public void setBuckets(int buckets) {
        this.buckets = buckets;
    }


}
