package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by zhudebin on 14-5-6.
 * 引擎作业状态枚举
 */
public enum JobStatusEnum {


    RUNNING(0),SUCCESS(1),FAIL(-1),KILLED(2);

    public int value;

    private JobStatusEnum(int type) {
        this.value = type;
    }
}
