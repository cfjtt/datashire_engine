package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by Juntao.Zhang on 2014/6/23.
 */
public enum CDCActiveFlag {
    ACTIVE("Y"),
    IN_ACTIVE("N");
    private String value;

    CDCActiveFlag(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
