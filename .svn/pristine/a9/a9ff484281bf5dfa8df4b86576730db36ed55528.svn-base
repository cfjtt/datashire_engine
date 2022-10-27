package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.spark.util.DestSystemHiveSquidUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * 落地hive
 * Created by zhudebin on 2017/5/3.
 */
public class TDestSystemHiveSquid extends TSquid {

    private static Log log = LogFactory.getLog(TDestSystemHiveSquid.class);

    {
        this.setType(TSquidType.DEST_SYSTEM_HIVE_SQUID);
    }

    TSquid preTSquid;
    String squidName;
    Map<Integer, TStructField> preId2Columns;

    // hive表的列名为key, 对应的上游的列信息为value  //必须和Hive列顺序一致
    List<Tuple2<String, TStructField>> hiveColumns;
    String tableName;

    SaveMode saveMode;

    @Override
    protected Object run(JavaSparkContext jsc) throws EngineException {
        Dataset<Row> ds = null;

        if(preId2Columns != null) {    // 判断是否可以直接使用 dataframe
            // 判断上游是否有outDataFrame输出
            if(preTSquid.outDataFrame != null ) {
                ds = preTSquid.outDataFrame;
            } else {
                // 没有dataframe的转换成Dataset
                ds = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                        squidName, preTSquid.getOutRDD().rdd(),
                        preId2Columns);
            }
        } else {
            throw new RuntimeException("翻译代码异常找程序员");
        }

        if(squidName.length() > 2) {
            int idx = squidName.lastIndexOf("_p");
            if(idx > 0) {
                String rePartitionNumStr = squidName.substring(idx + 2);
                try {
                    int rePartitionNum = Integer.parseInt(rePartitionNumStr);
                    int partitions = ds.rdd().partitions().length;
                    if(rePartitionNum < partitions) {
                        ds = ds.coalesce(rePartitionNum);
                    }
                } catch (Exception e) {
                    log.error("转换分区数异常,rePartitionNumStr:" + rePartitionNumStr);
                }
            }
        }

        DestSystemHiveSquidUtil.save(ds, hiveColumns, tableName, saveMode);
        return null;
    }

    public String getSquidName() {
        return squidName;
    }

    public void setSquidName(String squidName) {
        this.squidName = squidName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Tuple2<String, TStructField>> getHiveColumns() {
        return hiveColumns;
    }

    public void setHiveColumns(
            List<Tuple2<String, TStructField>> hiveColumns) {
        this.hiveColumns = hiveColumns;
    }

    public Map<Integer, TStructField> getPreId2Columns() {
        return preId2Columns;
    }

    public void setPreId2Columns(
            Map<Integer, TStructField> preId2Columns) {
        this.preId2Columns = preId2Columns;
    }

    public SaveMode getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(SaveMode saveMode) {
        this.saveMode = saveMode;
    }

    public TSquid getPreTSquid() {
        return preTSquid;
    }

    public void setPreTSquid(TSquid preTSquid) {
        this.preTSquid = preTSquid;
    }
}
