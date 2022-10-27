package com.eurlanda.datashire.engine.entity;

import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 *
 * 可以导出Exception的TSquid
 * Created by zhudebin on 16/2/23.
 */
public abstract class TExceptionDataSquid extends TSquid {

    // 异常数据输出RDD
    protected JavaRDD<Map<Integer, DataCell>> expOutRDD;
    // 异常是否需要处理
    protected boolean isExceptionHandle = false;

    public JavaRDD<Map<Integer, DataCell>> getExpOutRDD() {
        return expOutRDD;
    }

    protected void setExpOutRDD(
            JavaRDD<Map<Integer, DataCell>> expOutRDD) {
        this.expOutRDD = expOutRDD;
    }

    public boolean isExceptionHandle() {
        return isExceptionHandle;
    }

    public void setExceptionHandle(boolean exceptionHandle) {
        isExceptionHandle = exceptionHandle;
    }
}
