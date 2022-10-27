package com.eurlanda.datashire.engine.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhudebin on 14-5-20.
 * 对transformation的一层包装
 * 1.ttransformation 实际的操作
 * 2.rmKeys 操作完成后待删除的key
 */
public class TTransformationAction implements Serializable {

    // 实际的操作
    private TTransformation tTransformation;
    // 操作完成后待删除的key
    private HashSet<Integer> rmKeys;

    public TTransformationAction() {
    }

    public TTransformation gettTransformation() {
        return tTransformation;
    }

    public void settTransformation(TTransformation tTransformation) {
        this.tTransformation = tTransformation;
    }

    public Set<Integer> getRmKeys() {
        return rmKeys;
    }

    public void setRmKeys(HashSet<Integer> rmKeys) {
        this.rmKeys = rmKeys;
    }

    @Override public String toString() {
        return "TTransformationAction{" +
                "tTransformation=" + tTransformation +
                ", rmKeys=" + rmKeys +
                '}';
    }
}
