package com.eurlanda.datashire.engine.enumeration;

/**
 * Created by Juntao.Zhang on 2014/6/27.
 */
public enum RowDelimiterPosition {
    Begin(0), End(1);
    private int value;

    RowDelimiterPosition(int value) {
        this.value = value;
    }

    public static RowDelimiterPosition parse(int value) {
        for (RowDelimiterPosition p : RowDelimiterPosition.values()) {
            if (p.getValue() - value == 0) {
                return p;
            }
        }
        return null;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

}
