package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by Eurlanda on 2017/6/16.
 */
public enum ERRCode {
    //等待队列已满
    WAITQUEUE_ISMAX(80209),
    //非法的union，两个数据集列数不一致
    UNION_COLUMN_SIZE_IS_NOT_EQUALS(80211),
    //已达最大允许并行数
    PARALLEL_NUM_IS_MAX(80212);
    private int value;

    ERRCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
