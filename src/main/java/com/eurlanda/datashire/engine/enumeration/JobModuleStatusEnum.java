package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by zhudebin on 14-5-6.
 * SQUIDFLOW作业中各个模块运行的状态
 * -1：失败；0：成功；1...n 各种中间状态
 */
public enum  JobModuleStatusEnum {
    // 失败
    FAIL(-1),
    // 成功
    SUCCESS(0),
    // 中间状态1
    STAGE01(1),
    // 中间状态2
    STAGE02(2),
    // 中间状态3
    STAGE03(3),
    // 中间状态4
    STAGE04(4),
    // 中间状态5
    STAGE05(5);

    public int value;

    private JobModuleStatusEnum(int value) {
        this.value = value;
    }

}
