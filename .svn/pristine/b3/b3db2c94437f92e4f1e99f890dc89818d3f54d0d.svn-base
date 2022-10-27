package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TSamplingSquid extends TSquid{
    private static final long serialVersionUID = 1L;
    private static Log log = LogFactory.getLog(TSamplingSquid.class);
    {
        setType(TSquidType.SAMPLINGSQUID);
    }
    private List<TSamplingSquid> squidList = new ArrayList<>();
    private TSquid previousSquid;
    private double samplingPercent;  //抽样百分比
    private int sourceSquidId; //源squidid
    Map<Integer, TStructField> preId2Columns;
    Map<Integer, TStructField> currentId2Columns;

    public static double add(double v1,double v2){
        BigDecimal b1=new BigDecimal(Double.toString(v1));
        BigDecimal b2=new BigDecimal(Double.toString(v2));
        return b1.add(b2).doubleValue();
    }
    public static double sub(double d1,double d2){
        BigDecimal b1=new BigDecimal(Double.toString(d1));
        BigDecimal b2=new BigDecimal(Double.toString(d2));
        return b1.subtract(b2).doubleValue();

    }
    @Override
    protected Object run(JavaSparkContext jsc) throws EngineException {
        if(this.outRDD!=null){
            return this.outRDD;
        }
        Dataset<Row> ds = null;
        if(preId2Columns != null) {    // 判断是否使用dataframe来过滤
            // 判断上游是否有outDataFrame输出
            if(previousSquid.outDataFrame != null ) {
                ds = previousSquid.outDataFrame;
            } else {
                // 没有dataframe的转换成Dataset
                ds = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                        this.getName(), previousSquid.getOutRDD().rdd(),
                        preId2Columns);
            }
        } else {
            throw new RuntimeException("翻译代码异常找程序员");
        }

        //抽样 withReplacement true：抽样放回 false :抽样不放回(这里的意思是抽出来的数据不会重复)
        List<Double> weights = new ArrayList<>();
        double sum = 0d;
        for(TSamplingSquid squid : squidList){
            sum = add(sum,squid.samplingPercent);
            weights.add(squid.getSamplingPercent());
        }

        if(sum<1){
            weights.add(sub(1,sum));
        }
        double[] allWeights =  new double[weights.size()];
        for (int i=0;i<weights.size();i++){
            allWeights[i]=weights.get(i);
        }
        Dataset<Row>[] newDs = ds.randomSplit(allWeights);
        for(int i=0;i<squidList.size();i++){
            TSamplingSquid samplingSquid = squidList.get(i);
            samplingSquid.outDataFrame = newDs[i];
            samplingSquid.outRDD = TJoinSquid.groupTaggingDataFrameToRDD(newDs[i],samplingSquid.currentId2Columns).toJavaRDD();
        }
        return this.outRDD;
    }

    public double getSamplingPercent() {
        return samplingPercent;
    }

    public void setSamplingPercent(double samplingPercent) {
        this.samplingPercent = samplingPercent;
    }

    public int getSourceSquidId() {
        return sourceSquidId;
    }

    public void setSourceSquidId(int sourceSquidId) {
        this.sourceSquidId = sourceSquidId;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public Map<Integer, TStructField> getPreId2Columns() {
        return preId2Columns;
    }

    public void setPreId2Columns(Map<Integer, TStructField> preId2Columns) {
        this.preId2Columns = preId2Columns;
    }

    public Map<Integer, TStructField> getCurrentId2Columns() {
        return currentId2Columns;
    }

    public void setCurrentId2Columns(Map<Integer, TStructField> currentId2Columns) {
        this.currentId2Columns = currentId2Columns;
    }

    public List<TSamplingSquid> getSquidList() {
        return squidList;
    }

    public void setSquidList(List<TSamplingSquid> squidList) {
        this.squidList = squidList;
    }
}
