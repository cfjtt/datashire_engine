package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.enumeration.AggregateType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.util.*;

public class TPivotSquid extends TSquid{

    private List<String> groupColumns; //分组列
    private TColumn pivotColumn;  //povit
    private TColumn valueColumn;  //value
    private List<Object> pivotColumnValue;
    private int aggregationType;
    private TSquid previousSquid;
    Map<Integer, TStructField> preId2Columns;
    Map<Integer, TStructField> currentId2Columns;
    Map<Integer,TStructField> needTransColumns;
    {
        this.setType(TSquidType.PIVOTSQUID);
    }
    @Override
    protected Object run(JavaSparkContext jsc) throws EngineException {
        Dataset<Row> ds = null;
        if(preId2Columns != null) {    // 判断是否使用dataframe来过滤
            // 判断上游是否有outDataFrame输出
            if(previousSquid.outDataFrame != null ) {
                ds = previousSquid.outDataFrame;
            } else {
                // 没有dataframe的转换成Dataset
                //判断rdd中是否存在map和array类型，如果存在，那么去除这两个类型,因为这两个类型在pivotsquid中任何地方都不能使用
                if(preId2Columns!=null){
                    Iterator<Map.Entry<Integer,TStructField>> preId2Iter = preId2Columns.entrySet().iterator();
                    while(preId2Iter.hasNext()){
                        Map.Entry<Integer,TStructField> it = preId2Iter.next();
                        if(it.getValue().getDataType()==TDataType.ARRAY){
                            preId2Iter.remove();
                        } else if(it.getValue().getDataType() == TDataType.MAP){
                            preId2Iter.remove();
                        }
                    }
                }
                ds = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                        this.getName(), previousSquid.getOutRDD().rdd(),
                        preId2Columns);
            }
        } else {
            throw new RuntimeException("翻译代码异常找程序员");
        }

        //欲取一条数据，用来检测异常(傻子写法)
        ds.show(1);

        //聚合
        String aggName = AggregateType.valueOf(aggregationType).name();
        Map<String,String> aggExpr = new HashMap<>();
        aggExpr.put(valueColumn.getName(),aggName.toLowerCase());

        //分组
        List<Column> columns = new ArrayList<>();
        for(String columName : groupColumns){
            Column col = new Column(columName);
            columns.add(col);
        }
        ds.persist(StorageLevel.MEMORY_AND_DISK_SER());
        Dataset<Row> ds2 = ds.groupBy(scala.collection.JavaConversions.asScalaBuffer(columns).toSeq()).pivot(pivotColumn.getName(),pivotColumnValue).agg(aggExpr);
        ds2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        outRDD = TJoinSquid.groupTaggingDataFrameToRDD(ds2, this.needTransColumns).toJavaRDD();
        Dataset<Row> outDataSet =  TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                this.getName(), this.getOutRDD().rdd(),
                currentId2Columns);
        this.outDataFrame = outDataSet;
        ds.unpersist();
        ds2.unpersist();
        return null;
    }

    public List<String> getGroupColumns() {
        return groupColumns;
    }

    public void setGroupColumns(List<String> groupColumns) {
        this.groupColumns = groupColumns;
    }

    public TColumn getPivotColumn() {
        return pivotColumn;
    }

    public void setPivotColumn(TColumn pivotColumn) {
        this.pivotColumn = pivotColumn;
    }

    public TColumn getValueColumn() {
        return valueColumn;
    }

    public void setValueColumn(TColumn valueColumn) {
        this.valueColumn = valueColumn;
    }

    public List<Object> getPivotColumnValue() {
        return pivotColumnValue;
    }

    public void setPivotColumnValue(List<Object> pivotColumnValue) {
        this.pivotColumnValue = pivotColumnValue;
    }

    public int getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(int aggregationType) {
        this.aggregationType = aggregationType;
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

    public Map<Integer, TStructField> getNeedTransColumns() {
        return needTransColumns;
    }

    public void setNeedTransColumns(Map<Integer, TStructField> needTransColumns) {
        this.needTransColumns = needTransColumns;
    }


}
