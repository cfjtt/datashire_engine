package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.ud.UserDefinedSquid;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhudebin on 2017/4/24.
 */
public class TUserDefinedSquid extends TSquid {

    private static Logger logger = Logger.getLogger(TUserDefinedSquid.class);

    {
        this.setType(TSquidType.USER_DEFINED_SQUID);
    }

    String className;

    HashMap<String, String> params;

    TSquid preTSquid;
    String squidName;
    Map<Integer, TStructField> preId2Columns;
    Map<Integer, TStructField> currentId2Columns;

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
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

        try {
            UserDefinedSquid uds = (UserDefinedSquid)Class.forName(className).newInstance();
            uds.setParams(params);
            outDataFrame = uds.process(ds);
        } catch (Exception e) {
            logger.error("自定义squid运行异常,classname:" + className, e);
            throw new EngineException("该自定义squid类不存在," + className);
        } catch (Error e){
            logger.error(e.getMessage());
            throw new EngineException(e.getMessage());
        }
        outRDD = TJoinSquid.groupTaggingDataFrameToRDD(outDataFrame, currentId2Columns).toJavaRDD();
        return outRDD;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public HashMap<String, String> getParams() {
        return params;
    }

    public void setParams(HashMap<String, String> params) {
        this.params = params;
    }

    public TSquid getPreTSquid() {
        return preTSquid;
    }

    public void setPreTSquid(TSquid preTSquid) {
        this.preTSquid = preTSquid;
    }

    public String getSquidName() {
        return squidName;
    }

    public void setSquidName(String squidName) {
        this.squidName = squidName;
    }

    public Map<Integer, TStructField> getPreId2Columns() {
        return preId2Columns;
    }

    public void setPreId2Columns(
            Map<Integer, TStructField> preId2Columns) {
        this.preId2Columns = preId2Columns;
    }

    public Map<Integer, TStructField> getCurrentId2Columns() {
        return currentId2Columns;
    }

    public void setCurrentId2Columns(
            Map<Integer, TStructField> currentId2Columns) {
        this.currentId2Columns = currentId2Columns;
    }
}
