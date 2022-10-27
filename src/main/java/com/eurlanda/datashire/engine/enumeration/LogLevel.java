package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by Juntao.Zhang on 2014/5/28.
 */
public enum LogLevel {
    DEBUG(1),
    INFO(2),
    WARN(3),
    ERROR(4),;
    public int value;

    LogLevel(int value) {
        this.value = value;
    }


}
