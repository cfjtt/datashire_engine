package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.SquidUtil;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

/**
 * union 连接
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class TUnionSquid extends TSquid {
    private TUnionType unionType;
    private TSquid leftSquid;
    private TSquid rightSquid;
    //keep sort with right inKeyList one by one
    //left squid key list and outKeyList
    private List<Integer> leftInKeyList;
    private List<Integer> rightInKeyList;

    public TUnionSquid() {
        setType(TSquidType.UNION_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        if (leftSquid.getOutRDD() == null) {
            leftSquid.runSquid(jsc);
        }
        if (rightSquid.getOutRDD() == null) {
            rightSquid.runSquid(jsc);
        }
        // 判断是否已执行
        if (this.outRDD != null) {
            System.out.println("重复执行: squidId => " + getSquidId() + " " + getType());
            return outRDD;
        }
        JavaRDD<Map<Integer, DataCell>> leftRDD = leftSquid.getOutRDD();
        JavaRDD<Map<Integer, DataCell>> rightRDD = rightSquid.getOutRDD();

        JavaRDD<Map<Integer, DataCell>> resultRDD = SquidUtil.unionRDD(
                Lists.newArrayList(leftRDD, rightRDD),
                leftInKeyList, rightInKeyList, unionType);

        outRDD = resultRDD;
        return resultRDD;
    }

    public TSquid getLeftSquid() {
        return leftSquid;
    }

    public void setLeftSquid(TSquid leftSquid) {
        this.leftSquid = leftSquid;
    }

    public TSquid getRightSquid() {
        return rightSquid;
    }

    public void setRightSquid(TSquid rightSquid) {
        this.rightSquid = rightSquid;
    }

    public List<Integer> getLeftInKeyList() {
        return leftInKeyList;
    }

    public void setLeftInKeyList(List<Integer> leftInKeyList) {
        this.leftInKeyList = leftInKeyList;
    }

    public List<Integer> getRightInKeyList() {
        return rightInKeyList;
    }

    public void setRightInKeyList(List<Integer> rightInKeyList) {
        this.rightInKeyList = rightInKeyList;
    }

    public TUnionType getUnionType() {
        return unionType;
    }

    public void setUnionType(TUnionType unionType) {
        this.unionType = unionType;
    }
}
