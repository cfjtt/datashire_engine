package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.spark.util.DestCassandraSquidUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 落地CASSANDRA
 * Created by zhudebin on 2017/5/3.
 */
public class TDestCassandraSquid extends TSquid {

    {
        this.setType(TSquidType.DEST_CASSANDRA_SQUID);
    }

    TSquid preTSquid;
    String squidName;
    Map<Integer, TStructField> preId2Columns;

    // hive表的列名为key, 对应的上游的列信息为value
    List<Tuple2<String, TStructField>> destColumns;
    HashMap<String, String> cassandraConnectionInfo;
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

        DestCassandraSquidUtil.save(ds, destColumns, cassandraConnectionInfo, tableName, saveMode);
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

    public List<Tuple2<String, TStructField>> getDestColumns() {
        return destColumns;
    }

    public void setDestColumns(
            List<Tuple2<String, TStructField>> destColumns) {
        this.destColumns = destColumns;
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

    public HashMap<String, String> getCassandraConnectionInfo() {
        return cassandraConnectionInfo;
    }

    public void setCassandraConnectionInfo(
            HashMap<String, String> cassandraConnectionInfo) {
        this.cassandraConnectionInfo = cassandraConnectionInfo;
    }
}
